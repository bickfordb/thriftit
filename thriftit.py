"""Alternative version of the Python Thrift encodings/library"""

__author__ = "Brandon Bickford <bickfordb@gmail.com>"

import os
import sys
from struct import pack, unpack

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO as StringIO

try:
    import json
except ImportError:
    import simplejson as json

# Type IDs
T_BOOL   = 2 # Boolean 
T_BYTE   = 3 # Unsigned 8 Bit Integer
T_I8     = 3 # Signed 8 Bit Integer
T_DOUBLE = 4 # Signed 64 Bit Floating Point 
T_I16    = 6 # Signed 16 Bit Integer
T_I32    = 8 # Signed 32 Bit Integer
T_I64    = 10 # Signed 64 Bit Integers
T_STRING = 11 # Binary Strings
T_STRUCT = 12 # Structures (things with tagged fields)
T_MAP    = 13 # Map, a container mapping from one Thrift Type value to another Thrift Type value
T_SET    = 14 # Sets, a set of a Thrift Type values
T_LIST   = 15 # Lists, an array of Thrift Type values
T_UTF8   = 16 # A UTF-8 string
T_UTF16  = 17 # A UTF-16 string

# Encoding Symbols (some of these are the same as Type IDs)

SYM_STOP   = 0 # End of Struct/Message
SYM_VOID   = 1 # ? 
SYM_BOOL_FALSE = 1  
SYM_BOOL = SYM_BOOL_TRUE = 2 # Overloaded to be Boolean True in Compact Protocol
SYM_BYTE   = 3 # Signed 8 Bit Integer
SYM_DOUBLE = 4 # Signed 64 Bit Floating Point 
SYM_I16    = 6 # Signed 16 Bit Integer
SYM_I32    = 8 # Signed 32 Bit Integer
SYM_I64    = 10 # Signed 64 Bit Integers
SYM_STRING = 11 # Binary Strings
SYM_STRUCT = 12 # Structures (things with tagged fields)
SYM_MAP    = 13 # Map, a container mapping from one Thrift Type value to another Thrift Type value
SYM_SET    = 14 # Sets, a set of a Thrift Type values
SYM_LIST   = 15 # Lists, an array of Thrift Type values

_type_to_symbol = {
    T_BOOL   : SYM_BOOL,
    T_BYTE   : SYM_BYTE,
    T_DOUBLE : SYM_DOUBLE,
    T_I16    : SYM_I16,
    T_I32    : SYM_I32,
    T_I64    : SYM_I64,
    T_STRING : SYM_STRING,
    T_STRUCT : SYM_STRUCT,
    T_MAP    : SYM_MAP,
    T_SET    : SYM_SET,
    T_LIST   : SYM_LIST,
    T_UTF8   : SYM_STRING,
    T_UTF16  : SYM_STRING,
}

VERSION_MASK = -65536

class Codec(object):
    """Base codec class"""
    def dump(self, thrift_type, object, stream):
        """Encode an object to an output stream"""
        return self._dump_handlers[thrift_type.type_id](thrift_type, object, stream)

    def load(self, thrift_type, stream):
        """Decode an object from an input stream"""
        return self._load_handlers[thrift_type.type_id](thrift_type, stream)

    def dumps(self, thrift_type, object):
        """Encode an object and return a bytestring"""
        out = StringIO.StringIO()
        self.dump(thrift_type, object, out)
        return out.getvalue()

    def loads(self, thrift_type, buf):
        """Decode a bytestring to an object"""
        stream = StringIO.StringIO(buf)
        return self.load(thrift_type, stream)

    def __init__(self):
        self._dump_handlers = {
            T_BOOL   : self._dump_bool,
            T_BYTE   : self._dump_byte,
            T_I8    : self._dump_byte,
            T_DOUBLE : self._dump_double,
            T_I16    : self._dump_i16,
            T_I32    : self._dump_i32,
            T_I64    : self._dump_i64,
            T_STRING : self._dump_string,
            T_STRUCT : self._dump_struct,
            T_MAP    : self._dump_map,
            T_SET    : self._dump_set,
            T_LIST   : self._dump_list,
            T_UTF8   : self._dump_utf8,
            T_UTF16  : self._dump_utf16
        }

        self._load_handlers = {
            T_BOOL   : self._load_bool,
            T_BYTE   : self._load_byte,
            T_I8    : self._load_byte,
            T_DOUBLE : self._load_double,
            T_I16    : self._load_i16,
            T_I32    : self._load_i32,
            T_I64    : self._load_i64,
            T_STRING : self._load_string,
            T_STRUCT : self._load_struct,
            T_MAP    : self._load_map,
            T_SET    : self._load_set,
            T_LIST   : self._load_list,
            T_UTF8   : self._load_utf8,
            T_UTF16  : self._load_utf16
        }

class Error(Exception): 
    pass

class BinaryCodec(Codec):
    """Implement the binary codec"""
    def _dump_struct(self, thrift_type, object, stream):
        for name, field in thrift_type.fields().iteritems():
            symbol = _type_to_symbol[field.type.type_id]
            stream.write(pack('!BH', symbol, field.tag))
            self.dump(field.type, getattr(object, name), stream)
        stream.write(chr(SYM_STOP))

    def _load_struct(self, thrift_type, stream):
        name_fields = thrift_type.fields().iteritems()
        tag_to_field = dict((field.tag, (name, field)) for name, field in name_fields)
        vals = {}
        while True:
            if ord(stream.read(1)) == SYM_STOP:
                break
            tag, = unpack('!H', stream.read(2))
            name, field = tag_to_field[tag]
            vals[name] = self.load(field.type, stream)
        return thrift_type(**vals)

    def _load_map(self, thrift_type, stream):
        key_type = thrift_type.key_type
        value_type = thrift_type.value_type
        key_type_id, val_type_id, size = unpack('!BBi', stream.read(6))
        result = thrift_type()
        for i in xrange(size): 
            key = self.load(key_type, stream)
            value = self.load(value_type, stream)
            result[key] = value
        return result

    def _dump_map(self, thrift_type, object, stream):
        key_type = thrift_type.key_type
        value_type = thrift_type.value_type
       
        stream.write(pack('!BBi',
            _type_to_symbol[key_type.type_id],
            _type_to_symbol[value_type.type_id], len(object)))
        for key, value in object.iteritems():
            self.dump(key_type, key, stream)
            self.dump(value_type, value, stream)

    def _dump_utf8(self, thrift_type, object, stream): 
        self._dump_string(ByteStringType, object.encode('utf-8'), stream)

    def _dump_utf16(self, thrift_type, object, stream): 
        self._dump_string(ByteStringType, object.encode('utf-16'), stream)

    def _dump_bool(self, thrift_type, val, stream):
        stream.write(chr(SYM_BOOL_TRUE if val else SYM_BOOL_FALSE))

    def _dump_byte(self, thrift_type, val, stream):
        stream.write(pack("!B", val))

    def _dump_i16(self, thrift_type, val, stream):
        stream.write(pack("!h", val))

    def _dump_i32(self, thrift_type, val, stream):
        stream.write(pack("!i", val))

    def _dump_i64(self, thrift_type, val, stream):
        stream.write(pack("!q", val))

    def _dump_double(self, thrift_type, val, stream):
        stream.write(pack("!d", val))

    def _dump_string(self, thrift_type, val, stream):
        stream.write(pack("!I", len(val)) + val)

    def _dump_seq(self, thrift_type, object, stream):
        sym = _type_to_symbol[thrift_type.value_type.type_id]
        stream.write(pack('!BI', sym, len(object)))
        for value in object:
            self.dump(thrift_type.value_type, value, stream)

    _dump_set = _dump_seq
    _dump_list = _dump_seq

    def _load_seq(self, thrift_type, stream):
        etype, size = unpack('!BI', stream.read(5))
        items = []
        for i in xrange(size):
            yield self.load(thrift_type.value_type, stream)

    def _load_list(self, thrift_type, stream):
        return list(self._load_seq(thrift_type, stream))

    def _load_set(self, thrift_type, stream):
        return set(self._load_seq(thrift_type, stream))

    def _load_utf8(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-8')

    def _load_utf16(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-16')

    def _load_bool(self, thrift_type, stream):
        """Load a byte string"""
        return unpack('!B', stream.read(1))[0] != SYM_BOOL_FALSE 

    def _load_byte(self, thrift_type, stream):
        buf = stream.read(1)
        if not buf:
            raise Error("unexpected end of stream")
        val, = unpack('!B', buf)
        return val

    def _load_i16(self, thrift_type, stream):
        """Load a signed 16 bit integer"""
        buf = stream.read(2)
        if len(buf) != 2:
            raise Error("unexpected end of stream")
        val, = unpack('!h', buf)
        return val

    def _load_i32(self, thrift_type, stream):
        """Load a signed 32 bit integer"""
        buf = stream.read(4)
        if len(buf) != 4:
            raise Error("unexpected end of stream")
        val, = unpack('!i', buf)
        return val

    def _load_i64(self, thrift_type, stream):
        """Load a signed 64 bit integer"""
        buf = stream.read(8)
        if len(buf) != 8:
            raise Error("unexpected end of stream")
        val, = unpack('!q', buf)
        return val

    def _load_double(self, thrift_type, stream):
        """Load a IEEE 754 double"""
        buf = stream.read(8)
        if len(buf) != 8:
            raise Error("unexpected end of stream")
        val, = unpack('!d', buf)
        return val

    def _load_string(self, thrift_type, stream):
        """Load a byte string"""
        num = self._load_i32(I32Type, stream)
        assert num >= 0
        if num > 0:
            buf = stream.read(num)
            if len(buf) != num:
                raise Error("unexpected end of stream")
        else:
            buf = ''
        return buf

_by_tag = lambda (name, field): field.tag

class CompactCodec(Codec):
    """Thrift Compact Encoding"""

    def _dump_bool(self, thrift_type, val, stream):
        stream.write(chr(SYM_BOOL_TRUE if val else SYM_BOOL_FALSE))

    def _dump_struct(self, thrift_type, object, stream):
        last_field_id = 0
        
        for name, field in sorted(thrift_type.fields().iteritems(), key=_by_tag):
            # Boolean fields are written as part of the field into one byte:
            if field.type.type_id == T_BOOL:
                boolean_field = field
                val = getattr(object, name)
                the_type = SYM_BOOL_TRUE if val else SYM_BOOL_FALSE
            else:  
                the_type = field.type.type_id
            delta = field.tag - last_field_id
            if delta > 0 and delta <= 15:
                stream.write(chr((delta << 4) | the_type))
            else:
                stream.write(pack('!BH', the_type, field.tag))
            if field.type.type_id != T_BOOL:
                self.dump(field.type, getattr(object, name), stream)
            last_field_id = field.tag
        stream.write(chr(SYM_STOP))

    def _dump_seq(self, thrift_type, object, stream):
        """Dump a set or a list of values"""
        sz = len(object)
        elem_type = thrift_type.value_type.type_id
        if sz <= 14:
            stream.write(chr((sz << 4) | elem_type))

        else:
            stream.write(chr(0xF0 | elem_type))
            self._dump_varint(sz, stream)
        for value in object:
            self.dump(thrift_type.value_type, value, stream)

    _dump_list = _dump_seq
    _dump_set = _dump_seq

    def _dump_varint(self, num, stream):
        while True:
            if (num & ~0x7F) == 0:
                stream.write(chr(num & 0xFF)) 
                break
            else:
                stream.write(chr((num & 0x7F) | 0x80))
                num >>= 7

    def _dump_map(self, thrift_type, object, stream):
        size = len(object)
        key_type = thrift_type.key_type
        value_type = thrift_type.value_type
        self._dump_varint(size, stream)
        if size > 0:
            key_type_id = _type_to_symbol[key_type.type_id]
            value_type_id = _type_to_symbol[value_type.type_id]
            kv_type_id = (key_type_id << 4) | value_type_id
            stream.write(chr(kv_type_id))
            for key, value in object.iteritems():
                self.dump(key_type, key, stream)
                self.dump(value_type, value, stream)
        
    def _dump_i16(self, thrift_type, object, stream):
        self._dump_varint(int_to_zigzag(object), stream)

    def _dump_i32(self, thrift_type, object, stream):
        self._dump_varint(int_to_zigzag(object), stream)

    def _dump_i64(self, thrift_type, object, stream):
        self._dump_varint(long_to_zigzag(object), stream)

    def _dump_double(self, thrift_type, val, stream):
        stream.write(pack("!d", val))

    def _dump_utf8(self, thrift_type, val, stream):
        self._dump_string(thrift_type, val.encode('utf-8'), stream)

    def _dump_utf16(self, thrift_type, val, stream):
        self._dump_string(thrift_type, val.encode('utf-16'), stream)

    def _dump_string(self, thrift_type, val, stream):
        self._dump_varint(len(val), stream)
        stream.write(val)

    def _dump_byte(self, thrift_type, val, stream):
        stream.write(chr(val))

    def _load_bool(self, thrift_type, stream):
        return ord(stream.read(1)) == SYM_BOOL_TRUE

    def _load_byte(self, thrift_type, stream):
        return ord(stream.read(1))

    def _load_double(self, thrift_type, stream):
        return unpack('!d', stream.read(8))[0]

    def _load_i16(self, thrift_type, stream):
        return zigzag_to_int(self._load_varint(stream))

    _load_i32 = _load_i16

    def _load_i64(self, thrift_type, stream):
        return zigzag_to_int(self._load_varint(stream))

    def _load_string(self, thrift_type, stream):
        num = self._load_varint(stream)
        buf = stream.read(num)
        return buf

    def _load_utf8(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-8')

    def _load_utf16(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-16')

    def _load_struct(self, thrift_type, stream):
        name_fields = thrift_type.fields().iteritems()
        tag_to_field = dict((field.tag, (name, field)) for name, field in name_fields)
        vals = {}
        last_field_id = 0
        while True:
            delta_type = ord(stream.read(1))
            the_type = delta_type & 0x0f
            delta = (delta_type & 0xf0) >> 4

            if the_type == SYM_STOP:
                break

            if delta == 0:
                tag, = unpack('!H', stream.read(2))
            else:
                tag = delta + last_field_id

            name, field = tag_to_field[tag]
            if the_type == SYM_BOOL_TRUE or the_type == SYM_BOOL_FALSE:
                vals[name] = the_type == SYM_BOOL_TRUE
            else:
                vals[name] = self.load(field.type, stream)
            last_field_id = tag
        return thrift_type(**vals)

    def _load_map(self, thrift_type, stream):
        sz = self._load_varint(stream)
        result = thrift_type()
        if sz > 0:
            kv_type = ord(stream.read(1))
            for i in xrange(sz):
                key = self.load(thrift_type.key_type, stream)
                value = self.load(thrift_type.value_type, stream) 
                result[key] = value
        return result

    def _load_seq(self, thrift_type, stream):
        """Load a sequence container from a stream"""
        size_type = ord(stream.read(1))
        size = (size_type >> 4) & 0x0f
        type = size_type & 0x0f
        if size == 15:
            size = _load_varint(stream)
        value_type = thrift_type.value_type
        for i in xrange(size):
            yield self.load(value_type, stream)

    def _load_list(self, thrift_type, stream):
        """Load a list from a stream"""
        return list(self._load_seq(thrift_type, stream))

    def _load_set(self, thrift_type, stream):
        """Load a set from a stream"""
        return set(self._load_seq(thrift_type, stream))

    def _load_varint(self, stream):
        """Load a varint"""
        num = 0
        shift = 0 
        while True:
            byte = ord(stream.read(1))
            num |= (byte & 0x7f) << shift
            shift += 7
            if (byte & 0x80) == 0:
                break
        return num
    

def long_to_zigzag(num):
    """Convert a long to a zigzag integer"""
    num = long(num)
    return (num << 1) ^ (num >> 63)

def int_to_zigzag(num):
    """Convert an integer to zigzag integer"""
    num = int(num)
    return (num << 1) ^ (num >> 31)

def zigzag_to_int(num):
    """Convert a zigzag number to a normal number"""
    return (num >> 1) ^ -(num & 1) 

class Type(object):
    type_id = None

class I64Type(Type):
    """64 bit integers"""
    type_id = T_I64

class I32Type(Type):
    """32 bit integers"""
    type_id = T_I32

class I16Type(Type):
    """32 bit integers"""
    type_id = T_I16

class I8Type(Type):
    """32 bit integers"""
    type_id = T_I8

class ByteType(Type):
    """Bytes"""
    type_id = T_BYTE

class DoubleType(float, Type):
    """64 bit doubles"""
    type_id = T_DOUBLE

class UnicodeType(unicode, Type):
    """UnicodeType (encoded in binary as utf-8) variable length strings"""
    type_id = T_STRING

class ByteStringType(Type):
    """Binary variable length strings"""
    type_id = T_STRING

class BooleanType(Type):
    """Boolean values"""
    type_id = T_BOOL

class Field(object):
    """A Struct Field"""
    def __init__(self, type, tag, initial, optional):
        self.type = type
        if tag <= 0:
            raise ValueError("expected tag to be greater than 0")
        self.tag = tag
        self.initial = initial
        self.optional = optional

    def __repr__(self):
        return 'Field(%r, %r, %r, %r)' % (self.type, self.tag, self.initial, self.optional)

class StructType(type):
    """Metaclass for structs"""
    def __init__(self, name, bases, dictionary):
        self.__fields = {}
        for key, value in dictionary.items():
            if isinstance(value, Field):
                dictionary.pop(key)
                self.add_field(key, value)
        super(StructType, self).__init__(name, bases, dictionary)

    def add_field(self, name, field):
        if name in self.__fields:
            raise ValueError("field %r already defined" % (name, ))
        for field_i in self.__fields.itervalues():
            if field.tag == field_i.tag:
                raise ValueError("Field with tag %r already defined" % (field.tag, ))
        self.__fields[name] = field

    def fields(self):
        return self.__fields

    def __repr__(self):
        return '<%s.%s fields:%r at 0x%x>' % (self.__module__, self.__name__, self.fields(), id(self))

class Struct(Type):
    """Struct Type (things with named, tagged fields)"""
    __metaclass__ = StructType
    type_id = T_STRUCT

    def __init__(self, *args, **kwargs):
        if (len(args) > 1) or (args and kwargs):
            raise TypeError("unexpected arguments")
        if args:
            values = dict(args[0])
        else:
            values = kwargs

        for field_name, field in type(self).fields().iteritems():
            value = values.pop(field_name, field.initial())
            setattr(self, field_name, value)
        for key in kwargs:
            raise TypeError("unexpected keyword argument %r" % (key, ))

    def __repr__(self):
        class_name = self.__class__.__name__
        items = {}
        for name in type(self).fields():
            items[name] = getattr(self, name)
        return '%s(%r)' % (class_name, items)

    def dumps(self, codec):
        """Encode this object to a string using codec"""
        return codec.dumps(type(self), self)

    def dump(self, codec, stream):
        """Encode this object to a stream using codec"""
        return codec.dump(type(self), self, stream)

    @classmethod
    def loads(cls, codec, buf):
        """Decode this object from a string using codec"""
        return codec.loads(cls, buf)

    @classmethod
    def load(cls, codec, stream):
        """Decode this object from a stream using codec"""
        return codec.load(cls, stream)

class Exception(Struct, Exception):
    """Exception structures"""
    type_id = T_STRUCT

class MapType(dict, Type):
    type_id = T_MAP

    key_type = None
    value_type = None

    @classmethod
    def subtype(cls, key_type, value_type): 
        name = '%sOf%sTo%s' % (cls.__name__, key_type.__name__, value_type.__name__)
        return type(name, (cls, ), {'key_type': key_type, 'value_type': value_type})

class _SeqType(Type):
    value_type = None

    @classmethod
    def subtype(cls, value_type):
        name = value_type.__name__ + 'Of' + cls.__name__
        return type(name, (cls, ), {'value_type': value_type})

class ListType(list, _SeqType):
    type_id = T_LIST

class SetType(set, _SeqType):
    type_id = T_SET

class ServiceType(type):
    pass

class Service(Type):
    __metaclass__ = ServiceType

def _atom_type(atom_dict):
    atom_type = atom_dict.get("type")
    if atom_type is None:
        atom_type = ""
    atom_type = atom_type.lower()
    return atom_type

def types_from_json(jsbuf):
    type_config = json.loads(jsbuf)
    return types_from_config(type_config)

def _handle_enum(enum, the_atom, context):
    initial = 0 

def types_from_config(type_config):
    """Get a mapping of types from a configuration mapping

    Arguments:
    type_config -- dict, a type name to type configuration.  

    For example, the type configuration for a simple calculator AST might look like the following:
    {
        "BinaryExpression": {  
            "type": "struct",
            "fields": {
                "left": {
                    "type": "Expression",
                    "tag": 0
                }
                "op": {
                    "type": "Operator",
                    "tag": 1
                },
                "right": {
                    "type": "Expression",
                    "tag": 2
                }
            }
        },
        "Expression": {
            "type": "struct",
            "fields": {
                "number": {
                    "type": "double",
                    "tag": 0
                }
                "binary_expr": {
                    "type": "BinaryExpression",
                    "tag": 1
                }
        },
        "Operator": {
            "type": "enum",
            "values": {
                "PLUS": 0,
                "MINUS": 1,
            }
        }
    }
    """
    result = _default_types.copy()

    # Do a first pass so that recursive references work:
    for name, atom in type_config.iteritems():
        if isinstance(name, unicode):
            name = name.encode('utf-8')
        result[name] = type(name, (_atom_type_to_type[_atom_type(atom)], ), {})

    for name, atom in type_config.iteritems():
        atom_handler = _atom_type_to_handler[_atom_type(atom)]
        atom_handler(result[name], atom, result)
    return result

def _handle_struct(struct, atom, context):
    fields = atom.get("fields")
    if isinstance(fields, (tuple, list)):
        for idx, field_tup in enumerate(fields):
            if len(field_tup) > 2:
                options = field_tup[2]
            type_name = field_tup[1]
            field_name = field_tup[0]
            if type_name == "list":
                value_type_name = options["value"]
                list_type_name = "ListOf%s" % (value_type_name.title(), )
                if isinstance(list_type_name, unicode):
                    list_type_name = list_type_name.encode('utf-8')
                field_type = type(list_type_name, (List, ), {'value_type': context[value_type_name]})
            elif type_name == "set":
                value_type_name = options["value"]
                set_type_name = "SetOf%s" % (value_type_name.title(), )
                if isinstance(set_type_name, unicode):
                    set_type_name = set_type_name.encode('utf-8')
                field_type = type(set_type_name, (SetType, ), {'value_type': context[value_type_name]})
            elif type_name == "map":
                value_type_name = options["value"]
                key_type_name = options["key"]
                map_type_name = "MapFrom%sTo%s" % (key_type_name.title(), value_type_name.title())
                value_type = context[value_type_name]
                key_type = context[key_type_name]
                if isinstance(map_type_name, unicode):
                    map_type_name = map_type_name.encode('utf-8')
                field_type = type(map_type_name, (Map, ), {'value_type': value_type, 'key_type': key_type})
            else: 
                field_type = context[type_name]
            options = {}
            default = options.get("default")
            tag = options.get("tag", idx)
            optional = options.get("optional", False)
            optional = bool(optional)
            struct.add_field(field_name, Field(field_type, tag, _bind(default), optional))
    else:
        raise TypeError("unexpected field sequence")

def d(x):
    return lambda: x

class Enum(int, I32Type):
    pass 

_atom_type_to_type = {
    'struct': Struct,
    'enum': Enum,
    'service': Service,
}

_atom_type_to_handler = { 
    'struct': _handle_struct,
    'enum': _handle_enum,
}


_default_types = {
    'byte': ByteType,
    'i8': I8Type,
    'i16': I16Type,
    'i32': I32Type,
    'i64': I64Type,
    'string': UnicodeType,
    'binary': ByteStringType,
    'double': DoubleType,
    'bool': BooleanType,
}
