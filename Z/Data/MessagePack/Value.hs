{- |
Module    : Z.Data.MessagePack.Value
Description : MessagePack object definition and parser
Copyright : (c) Hideyuki Tanaka 2009-2015
          , (c) Dong Han 2020
License   : BSD3
-}
module Z.Data.MessagePack.Value(
  -- * MessagePack Value
    Value(..)
    -- * parse into MessagePack Value
  , parseValue
  , parseValue'
    -- * Value Parsers
  , value
    -- * Convert JSON & MessagePack Values
  , toJSONValue
  ) where

import           Control.DeepSeq
import           Control.Monad
import           Data.Bits
import           Data.Int
import qualified Data.Scientific            as Sci
import           Data.Word
import           GHC.Exts
import           GHC.Integer.GMP.Internals
import           GHC.Stack
import           GHC.Generics               (Generic)
import           Test.QuickCheck.Arbitrary  (Arbitrary, arbitrary)
import qualified Test.QuickCheck.Gen        as Gen
import           Prelude                    hiding (map)
import qualified Z.Data.Array               as A
import qualified Z.Data.Text                as T
import qualified Z.Data.Parser              as P
import qualified Z.Data.Vector.Base         as V
import qualified Z.Data.JSON                as J
import           Z.Data.JSON.Value          (floatToScientific, doubleToScientific)

-- | Representation of MessagePack data.
data Value
    = Bool                  !Bool                   -- ^ true or false
    | Int    {-# UNPACK #-} !Int64                  -- ^ an integer
    | Float  {-# UNPACK #-} !Float                  -- ^ a floating point number
    | Double {-# UNPACK #-} !Double                 -- ^ a floating point number
    | Str    {-# UNPACK #-} !T.Text                 -- ^ a UTF-8 string
    | Bin    {-# UNPACK #-} !V.Bytes                -- ^ a byte array
    | Array  {-# UNPACK #-} !(V.Vector Value)       -- ^ a sequence of objects
    | Map    {-# UNPACK #-} !(V.Vector (Value, Value)) -- ^ key-value pairs of objects
    | Ext    {-# UNPACK #-} !Word8                  -- ^ type tag
             {-# UNPACK #-} !V.Bytes                -- ^ data payload
    | Nil                                           -- ^ nil
  deriving (Show, Eq, Ord, Generic)
  deriving anyclass T.Print

instance NFData Value where
    rnf obj = case obj of
        Array a -> rnf a
        Map   m -> rnf m
        _             -> ()

instance Arbitrary Value where
    arbitrary = Gen.sized $ \n -> Gen.oneof
        [ Bool   <$> arbitrary
        , Int    <$> negatives
        , Float  <$> arbitrary
        , Double <$> arbitrary
        , Str    <$> arbitrary
        , Bin    <$> arbitrary
        , Array  <$> Gen.resize (n `div` 2) arbitrary
        , Map    <$> Gen.resize (n `div` 4) arbitrary
        , Ext    <$> arbitrary <*> arbitrary
        , pure Nil
        ]
        where negatives = Gen.choose (minBound, -1)


value :: P.Parser Value
{-# INLINABLE value #-}
value = do
    tag <- P.anyWord8
    case tag of
        -- Nil
        0xC0 -> return Nil

        -- Bool
        0xC2 -> return (Bool False)
        0xC3 -> return (Bool True)

        -- Integer
        c | c .&. 0x80 == 0x00 -> return (Int (fromIntegral c))
          | c .&. 0xE0 == 0xE0 -> return (Int (fromIntegral (fromIntegral c :: Int8)))

        0xCC -> Int . fromIntegral <$> P.anyWord8
        0xCD -> Int . fromIntegral <$> P.decodePrimBE @Word16
        0xCE -> Int . fromIntegral <$> P.decodePrimBE @Word32
        0xCF -> Int . fromIntegral <$> P.decodePrimBE @Word64

        0xD0 -> Int . fromIntegral <$> P.decodePrim @Int8
        0xD1 -> Int . fromIntegral <$> P.decodePrimBE @Int16
        0xD2 -> Int . fromIntegral <$> P.decodePrimBE @Int32
        0xD3 -> Int . fromIntegral <$> P.decodePrimBE @Int64

        -- Float
        0xCA -> Float <$> P.decodePrimBE @Float
        -- Double
        0xCB -> Double <$> P.decodePrimBE @Double

        -- String
        t | t .&. 0xE0 == 0xA0 -> str (t .&. 0x1F)
        0xD9 -> str =<< P.anyWord8
        0xDA -> str =<< P.decodePrimBE @Word16
        0xDB -> str =<< P.decodePrimBE @Word32

        -- Binary
        0xC4 -> bin =<< P.anyWord8
        0xC5 -> bin =<< P.decodePrimBE @Word16
        0xC6 -> bin =<< P.decodePrimBE @Word32

        -- Array
        t | t .&. 0xF0 == 0x90 -> array (t .&. 0x0F)
        0xDC -> array =<< P.decodePrimBE @Word16
        0xDD -> array =<< P.decodePrimBE @Word32

        -- Map
        t | t .&. 0xF0 == 0x80 -> map (t .&. 0x0F)
        0xDE -> map =<< P.decodePrimBE @Word16
        0xDF -> map =<< P.decodePrimBE @Word32

        -- Ext
        0xD4 -> ext (1  :: Int)
        0xD5 -> ext (2  :: Int)
        0xD6 -> ext (4  :: Int)
        0xD7 -> ext (8  :: Int)
        0xD8 -> ext (16 :: Int)
        0xC7 -> ext =<< P.anyWord8
        0xC8 -> ext =<< P.decodePrimBE @Word16
        0xC9 -> ext =<< P.decodePrimBE @Word32

        -- impossible
        x -> P.fail' ("Z.Data.MessagePack: unknown tag " <> T.toText x)

  where
    str !l = do
        bs <- P.take (fromIntegral l)
        case T.validateMaybe bs of
            Just t -> return (Str t)
            _  -> P.fail' "Z.Data.MessagePack: illegal UTF8 Bytes"
    bin !l   = Bin <$> P.take (fromIntegral l)
    array !l = Array . V.packN (fromIntegral l) <$> replicateM (fromIntegral l) value
    map !l   = Map . V.packN (fromIntegral l) <$> replicateM (fromIntegral l) ((,) <$> value <*> value)
    ext !l   = Ext <$> P.decodePrim <*> P.take (fromIntegral l)

-- | Parse 'Value' without consuming trailing bytes.
parseValue :: V.Bytes -> (V.Bytes, Either P.ParseError Value)
{-# INLINE parseValue #-}
parseValue = P.parse value

-- | Parse 'Value', if there're bytes left, parsing will fail.
parseValue' :: V.Bytes -> Either P.ParseError Value
{-# INLINE parseValue' #-}
parseValue' = P.parse' (value <* P.endOfInput)

-- | Convert MessagePack's 'Value' to JSON 'J.Value'.
--
-- There're some conventions on 'toJSONValue' function:
--
--  * MessagePack's 'Map' support arbitrary key types, while JSON 'J.Object' only support text keys,
--    so if a non-text key is met, it will be converted to JSON String first, then prepended with @__messagepack_complex_key_@,
--    otherwise it will be used as it is.
--
--  * MessagePack's 'Bin' type will be converted into a JSON object with a @__base64@ field.
--
--  * MessagePack's 'Ext' type will be converted into JSON Number if the tag is 0x00 or 0x01(see documents in "Z.Data.MessagePack"
--    for more details on how @Z-MessagePack@ encode scientific numbers with ext format), otherwise will be converted to JSON
--    object @{"__ext":{"__tag": ..., "__payload": ...}}@.
--
--  * Other types will have obvious behaviors: 'Bool' maps to 'J.Bool'; 'Int', 'Float' and 'Double' map to 'J.Number',
--    'Str' maps to 'J.String', 'Array' maps to 'J.Array' and 'Nil' maps to 'J.Null'.
--
toJSONValue :: HasCallStack => Value -> J.Value
{-# INLINABLE toJSONValue #-}
toJSONValue (Bool b  ) = J.Bool b
toJSONValue (Int  i  ) = J.Number $! fromIntegral i
toJSONValue (Float f ) = J.Number $! floatToScientific f
toJSONValue (Double d) = J.Number $! doubleToScientific d
toJSONValue (Str t   ) = J.String t
toJSONValue (Bin b   ) = J.object $ [ "__base64" J..=  b ]
toJSONValue (Array v ) = J.Array $! toJSONValue <$> v
toJSONValue (Map m   ) = J.Object $! (\ (k, v) -> let !k' = case v of
                                                        Str v' -> J.String v'
                                                        _ -> "__messagepack_complex_key_" <> J.encodeText (toJSONValue v)
                                                      !v' = toJSONValue v
                                                  in (k', v')) <$> m
toJSONValue (Ext tag payload)
    | tag <= 0x01 =
        case P.parse value payload of
            ((V.PrimVector (A.PrimArray ba#) (I# s#) (I# l#)) , Right (Int d)) ->
                let !c = importIntegerFromByteArray ba# (int2Word# s#) (int2Word# l#) 1#
                    !e = fromIntegral d
                in if tag == 0x01 then J.Number $! negate (Sci.scientific c e)
                                  else J.Number $! Sci.scientific c e
            _ -> error "converting MessagePack value to JSON value failed: illegal Ext 00/01 payload."
    | otherwise = J.object [ "__ext" J..= J.object [ "__tag" J..= tag, "__payload" J..= payload ]]
toJSONValue Nil        = J.Null
