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
            
class CodecTestCase(unittest.TestCase):

    def _run_test(self, codec):
        f = Foo(num=25.1, msg=u'Hi, how are you', bool_true=True, bool_false=False, int_large=2**31 - 1, int_small=5, int_neg=-23, long_large=2**62, long_neg=-44)
        self.assertEquals(f.msg, u'Hi, how are you')
        self.assertEquals(f.num, 25.1)
        buf = f.dumps(codec)
        g = Foo.loads(codec, buf)
        self.assertEquals(g.msg, f.msg)
        self.assertEquals(g.bool_true, f.bool_true)
        self.assertEquals(g.bool_false, f.bool_false)
        self.assertEquals(g.int_large, f.int_large)
        self.assertEquals(g.int_small, f.int_small)
        self.assertEquals(g.int_neg, f.int_neg)
        self.assertEquals(g.long_large, f.long_large)
        self.assertEquals(g.long_neg, f.long_neg)
        self.assertEquals(g.num, f.num)

class BinaryCodecTestCase(CodecTestCase):
    def test(self):
        self._run_test(thriftit.BinaryCodec())

class CompactCodecTestCase(CodecTestCase):
    def test(self):
        self._run_test(thriftit.CompactCodec())

if __name__ == '__main__':
    unittest.main()


