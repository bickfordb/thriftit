Re-implementation of the Thrift Python library

Rough goals: 

 * Binary & Compact encodings
 * Define & Export protocols using JSON, Python, Thrift IDL and YAML
 * More straightforward libraries (e.g. "thriftit.BinaryCodec()" instead of "thrift.protocol.TBinaryProtocol.TBinaryProtocolFactory()")
 * Try to fit in with Python better.  For example, prefer `dump` & `load` over `write` & `read` and prefer underscored names over camelcase.
 * Cyclic structs (named an anti-feature on the Thrift Wiki):

          struct Cons { 
              1: binary head,
              2: Cons tail optional
          }

 * Parameterized Structs / Containers

          struct Tree<A> {
              1: Tree<A> left optional,
              2: Tree<A> right optional,
              3: A val required
          }

          struct Code { 
              1: string file, 
              2: i32 line_num,
              3: Tree<Expr> expr
          }

 * Pure Python `.thrift` Parser
 * Support alternative encodings where possible (Avro, Protocol Buffers, XDR, YAML, JSON)
 * Tornado plugins

Implemented so far:

 * Define protocol spec in JSON/YAML/Python
 * Binary codec
 * Compact codec
 * Cyclic structs
 * Dynamically define structs in Python:

        class Enum(int):
           ...

        class MethodEnum(Enum):
           GET = 1
           POST = 2
           HEAD = 3
           DELETE = 4

        class HTTPRequest(thriftit.Struct):
            addr = thriftit.Field(thriftit.I64Type, 1, int, 0)
            method = thriftit.Field(thriftit.I8Type, 2, MethodEnum, MethodEnum.GET)
            uri = thriftit.Field(thriftit.ByteStringType, 3, str, '')
            body = thriftitit.Field(thriftit.ByteStringType, 4, str, '')

