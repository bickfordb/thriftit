import unittest

import thriftit


class BasicTestCase(unittest.TestCase):
    def test(self):
        class StructExample(thriftit.Struct):
            num = thriftit.Field(thriftit.I32Type, 0, int, False)

        example = StructExample(num=33)
        assert example.num == 33

        class BroError(thriftit.Exception):
            bros = thriftit.Field(thriftit.I32Type, 0, int, False)

        try:
            raise BroError(bros=5)
        except BroError, val:
            assert val.bros == 5

class Foo(thriftit.Struct):
    num = thriftit.Field(thriftit.DoubleType, 0, float, False)
    msg = thriftit.Field(thriftit.UnicodeType, 1, unicode, False)
            
class CodecTestCase(unittest.TestCase):

    def _run_test(self, codec):
        f = Foo(num=25.1, msg=u'Hi, how are you')
        self.assertEquals(f.msg, u'Hi, how are you')
        self.assertEquals(f.num, 25.1)
        buf = f.dumps(codec)
        
        g = Foo.loads(codec, buf)

        self.assertEquals(g.num, f.num)
        self.assertEquals(g.msg, f.msg)

class BinaryCodecTestCase(CodecTestCase):
    def test(self):
        self._run_test(thriftit.BinaryCodec())

class CompactCodecTestCase(CodecTestCase):
    def test(self):
        self._run_test(thriftit.CompactCodec())


if __name__ == '__main__':
    unittest.main()


