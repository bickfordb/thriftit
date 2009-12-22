import StringIO
import unittest

import thriftit

class BasicTestCase(unittest.TestCase):
    def test(self):
        class StructExample(thriftit.Struct):
            num = thriftit.Field(thriftit.I32Type, 1, int, False)

        example = StructExample(num=33)
        assert example.num == 33

        class BroError(thriftit.Exception):
            bros = thriftit.Field(thriftit.I32Type, 1, int, False)

        try:
            raise BroError(bros=5)
        except BroError, val:
            assert val.bros == 5

class Foo(thriftit.Struct):
    bool_true = thriftit.Field(thriftit.BooleanType, 1, bool, False)
    msg = thriftit.Field(thriftit.UnicodeType, 2, unicode, False)
    bool_false = thriftit.Field(thriftit.BooleanType, 3, bool, False)
    int_large = thriftit.Field(thriftit.I32Type, 4, int, False)
    int_neg = thriftit.Field(thriftit.I32Type, 5, int, False)
    int_small = thriftit.Field(thriftit.I32Type, 6, int, False)
    long_large = thriftit.Field(thriftit.I64Type, 7, int, False)
    long_neg = thriftit.Field(thriftit.I64Type, 8, int, False)
    num = thriftit.Field(thriftit.DoubleType, 9, float, False)
    numbers = thriftit.Field(thriftit.ListType.subtype(thriftit.DoubleType), 10, list, False)
    friends = thriftit.Field(thriftit.SetType.subtype(thriftit.UnicodeType), 11, set, False)
    age_to_person = thriftit.Field(thriftit.MapType.subtype(thriftit.DoubleType, thriftit.UnicodeType), 12, dict, False)
            
class CodecTestCase:
    def test_struct(self):
        codec = self.codec
        f = Foo(num=25.1, msg=u'Hi, how are you', bool_true=True, bool_false=False, int_large=2**31 - 1, int_small=5, int_neg=-23, long_large=2**62, long_neg=-44, numbers=[13.5, 25.3], friends=set(['Alice', 'Bob']), age_to_person={15:'Alice', 16: 'Bob'})
        self.assertEquals(f.msg, u'Hi, how are you')
        self.assertEquals(f.num, 25.1)
        buf = f.dumps(codec)
        sio = StringIO.StringIO(buf)
        g = Foo.load(codec, sio)
        self.assertEquals(sio.read(1), '') # The buffer should be empty
        self.assertEquals(g.msg, f.msg)
        self.assertEquals(g.bool_true, f.bool_true)
        self.assertEquals(g.bool_false, f.bool_false)
        self.assertEquals(g.int_large, f.int_large)
        self.assertEquals(g.int_small, f.int_small)
        self.assertEquals(g.int_neg, f.int_neg)
        self.assertEquals(g.long_large, f.long_large)
        self.assertEquals(g.long_neg, f.long_neg)
        self.assertEquals(g.num, f.num)
        self.assertEquals(g.friends, f.friends)
        self.assertEquals(g.numbers, f.numbers)

    def test_boolean(self):
        self._assert_round_trip(thriftit.BooleanType, True)
        self._assert_round_trip(thriftit.BooleanType, False)

    def test_i16(self):
        for number in [-1, 0, 1, 1 << 14, -1 * (1 << 14)]:
            self._assert_round_trip(thriftit.I16Type, number)

    def test_i32(self):
        for number in [-1, 0, 1, 1 << 30, -1 * (1 << 30)]:
            self._assert_round_trip(thriftit.I32Type, number)

    def test_i64(self):
        for number in [-1, 0, 1, 1 << 62, -1 * (1 << 62)]:
            self._assert_round_trip(thriftit.I64Type, number)

    def test_byestring(self):
        self._assert_round_trip(thriftit.ByteStringType, '')
        self._assert_round_trip(thriftit.ByteStringType, chr(244))
        self._assert_round_trip(thriftit.ByteStringType, chr(0))
        self._assert_round_trip(thriftit.ByteStringType, 'hi how are you')

    def test_unicode(self):
        self._assert_round_trip(thriftit.UnicodeType, u'')
        self._assert_round_trip(thriftit.UnicodeType, u'hi how are you')
        self._assert_round_trip(thriftit.UnicodeType, u'hi  how are you')


    def _assert_round_trip(self, thrift_type, value):
        buf = self.codec.dumps(thrift_type, value)
        expected_value = self.codec.loads(thrift_type, buf)
        self.assertEquals(value, expected_value)

class BinaryCodecTestCase(CodecTestCase, unittest.TestCase):
    @property
    def codec(self):
        return thriftit.BinaryCodec()

class CompactCodecTestCase(CodecTestCase, unittest.TestCase):
    @property
    def codec(self):
        return thriftit.CompactCodec()

if __name__ == '__main__':
    unittest.main()


