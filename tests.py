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
            
class BinaryCodecTest(unittest.TestCase):
    def test(self):
        f = Foo(num=25.1, msg=u'Hi, how are you')
        self.assertEquals(f.msg, u'Hi, how are you')
        self.assertEquals(f.num, 25.1)
        codec = thriftit.BinaryCodec()
        buf = f.dumps(codec)
        
        g = Foo.loads(codec, buf)

        self.assertEquals(g.num, f.num)
        self.assertEquals(g.msg, f.msg)

if __name__ == '__main__':
    unittest.main()


