Re-implementation of the Thrift Python library for Python

Rough goals: 

 * Binary & Compact encodings
 * Define & Export protocols using JSON, Python, Thrift IDL and YAML
 * Simpler libraries (e.g. "thriftit.BinaryCodec()" instead of "thrift.protocol.TBinaryProtocol.TBinaryProtocolFactory()")
 * Try to fit in with Python better.  For eaxmple, use "dump" instead of "write" or "load", and use "underscored_method_names" instead of "camelCase"
 * Sneak in useful features: 
   * Cyclic structs
       struct ConsBuf { 
            1: binary buf,
            2: ConsBuf next optional
        }

 * Support other alternative encodings where possible (Avro, Protocol Buffers, XDR, YAML, JSON)
 * Tornado plugins

Implemented so far:

 * define protocol spec in JSON/YAML/Python
 * "Binary" protocol
 * Cyclic structs

