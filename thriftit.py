"""Hopefully less strange version of the Python Thrift encodings/library"""

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

STOP   = 0
VOID   = 1
BOOL   = 2
BYTE   = 3
I08    = 3
DOUBLE = 4
I16    = 6
I32    = 8
I64    = 10
STRING = 11
UTF7   = 11
STRUCT = 12
MAP    = 13
SET    = 14
LIST   = 15
UTF8   = 16
UTF16  = 17

VERSION_MASK = -65536

class Codec(object):
    """Base codec class"""
    def dump(self, thrift_type, object, stream):
        """Encode an object to an output stream"""
        return self._dump_handlers[thrift_type._thrift_type_id](thrift_type, object, stream)

    def load(self, thrift_type, stream):
        """Decode an object from an input stream"""
        return self._load_handlers[thrift_type._thrift_type_id](thrift_type, stream)

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
            BOOL   : self._dump_bool,
            BYTE   : self._dump_byte,
            I08    : self._dump_byte,
            DOUBLE : self._dump_double,
            I16    : self._dump_i16,
            I32    : self._dump_i32,
            I64    : self._dump_i64,
            STRING : self._dump_string,
            UTF7   : self._dump_utf7,
            STRUCT : self._dump_struct,
            MAP    : self._dump_map,
            SET    : self._dump_set,
            LIST   : self._dump_list,
            UTF8   : self._dump_utf8,
            UTF16  : self._dump_utf16
        }

        self._load_handlers = {
            BOOL   : self._load_bool,
            BYTE   : self._load_byte,
            I08    : self._load_byte,
            DOUBLE : self._load_double,
            I16    : self._load_i16,
            I32    : self._load_i32,
            I64    : self._load_i64,
            STRING : self._load_string,
            UTF7   : self._load_utf7,
            STRUCT : self._load_struct,
            MAP    : self._load_map,
            SET    : self._load_set,
            LIST   : self._load_list,
            UTF8   : self._load_utf8,
            UTF16  : self._load_utf16
        }

class Error(Exception): 
    pass

class BinaryCodec(Codec):

    def _dump_struct(self, thrift_type, object, stream):
        for name, field in thrift_type.fields().iteritems():
            self._dump_byte(ByteType, field.type._thrift_type_id, stream)
            self._dump_i16(I16Type, field.tag, stream)
            self.dump(field.type, getattr(object, name), stream)
        self._dump_byte(ByteType, STOP, stream)

    def _load_struct(self, thrift_type, stream):
        name_fields = thrift_type.fields().iteritems()
        tag_to_field = dict((field.tag, (name, field)) for name, field in name_fields)
        vals = {}
        while True:
            type = self._load_byte(ByteType, stream)
            if type == STOP:
                break
            tag = self._load_i16(I16Type, stream)
            name, field = tag_to_field[tag]
            vals[name] = self.load(field.type, stream)
        return thrift_type(**vals)

    def _load_map(self, thrift_type, object, stream):
        key_type = thrift_type.key_type
        value_type = thrift_type.value_type

    def _dump_map(self, thrift_type, object, stream):
        key_type = thrift_type.key_type
        value_type = thrift_type.value_type
       
        self._dump_byte(ByteType, key_type._thrift_type_id, stream)
        self._dump_byte(ByteType, value_type._thrift_type_id, stream)
        self._dump_i32(I32Type, len(object), strema)
     
        for key, value in object.iteritems():
            self.dump(key_type, key, stream)
            self.dump(value_type, value, stream)

    def _dump_utf8(self, thrift_type, object, stream): 
        self._dump_string(ByteStringType, object.encode('utf-8'), stream)

    def _dump_utf7(self, thrift_type, object, stream): 
        self._dump_string(ByteStringType, object.encode('utf-7'), stream)

    def _dump_utf16(self, thrift_type, object, stream): 
        self._dump_string(ByteStringType, object.encode('utf-16'), stream)

    def _dump_message(self, name, thrift_type_id, sequence_id, stream):
        self._dump_i32(VERSION_1 | thrift_type_id, stream)
        self._dump_string(name, stream)
        self._dump_i32(sequence_id, stream)

    
    def _dump_bool(self, thrift_type, val, stream):
        self._dump_byte(ByteType, 1 if val else 0, stream)

    def _dump_byte(self, thrift_type, val, stream):
        stream.write(pack("!b", val))

    def _dump_i16(self, thrift_type, val, stream):
        stream.write(pack("!h", val))

    def _dump_i32(self, thrift_type, val, stream):
        stream.write(pack("!i", val))

    def _dump_i64(self, thrift_type, val, stream):
        stream.write(pack("!q", val))

    def _dump_double(self, thrift_type, val, stream):
        stream.write(pack("!d", val))

    def _dump_string(self, thrift_type, val, stream):
        stream.write(pack("!i", len(val)) + val)

    def _dump_seq(self, thrift_type, object, stream):
        self._dump_byte(ByteType, ttype.value_type._thrift_type_id, stream)
        self._dump_i32(size, stream)
        for value in object:
            self.dump(value_type, value, stream)

    _dump_set = _dump_seq
    _dump_list = _dump_seq

    def _load_message(self, thrift_type, stream):
        sz = self._load_i32(stream)
        if sz < 0:
            version = sz & VERSION_MASK
            if version != VERSION_1:
                raise Error("unexpected version (%d)" % (version, ))
            type = sz & TYPE_MASK
            name = self._load_string(stream)
            sequence_id = self._load_i32(stream)
        else:
            raise Error("missing version mask")
        return (name, type, sequence_id)

    def _load_field(self, thrift_type, stream):
        type = self._load_byte(stream)
        id = self._load_i16(stream) if type != STOP else 0
        return (None, type, id)

    def _load_map_begin(self, thrift_type, stream):
        ktype = self._load_byte(stream)
        vtype = self._load_byte(stream)
        size = self._load_i32(stream)
        return (ktype, vtype, size)

    def _load_list(self, thrift_type, stream):
        etype = self._load_byte(stream)
        size = self._load_i32(stream)
        return (etype, size)

    def _load_utf7(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-7')

    def _load_utf8(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-8')

    def _load_utf16(self, thrift_type, stream):
        return self._load_string(thrift_type, stream).decode('utf-16')

    def _load_set(self, thrift_type, stream):
        etype = self._load_byte(stream)
        size = self._load_i32(stream)
        result = set()
        value_type = thrift_type.value_type
        for i in xrange(size):
            value = self.load(value_type, stream)
            result.add(value)
        return result

    def _load_bool(self, thrift_type, stream):
        return self._load_byte(stream) != 0 

    def _load_byte(self, thrift_type, stream):
        buf = stream.read(1)
        if not buf:
            raise Error("unexpected end of stream")
        val, = unpack('!b', buf)
        return val

    def _load_i16(self, thrift_type, stream):
        buf = stream.read(2)
        if len(buf) != 2:
            raise Error("unexpected end of stream")
        val, = unpack('!h', buf)
        return val

    def _load_i32(self, thrift_type, stream):
        buf = stream.read(4)
        if len(buf) != 4:
            raise Error("unexpected end of stream")
        val, = unpack('!i', buf)
        return val

    def _load_i64(self, thrift_type, stream):
        buf = stream.read(8)
        if len(buf) != 8:
            raise Error("unexpected end of stream")
        val, = unpack('!q', buf)
        return val

    def _load_double(self, thrift_type, stream):
        buf = stream.read(8)
        if len(buf) != 8:
            raise Error("unexpected end of stream")
        val, = unpack('!d', buf)
        return val

    def _load_string(self, thrift_type, stream):
        num = self._load_i32(I32Type, stream)
        assert num >= 0
        if num > 0:
            buf = stream.read(num)
            if len(buf) != num:
                raise Error("unexpected end of stream")
        else:
            buf = ''
        return buf

class CompactCodec(Codec):
    pass
    
class Type(object):
    pass

class I64Type(Type):
    """64 bit integers"""
    _thrift_type_id = I64

class I32Type(Type):
    """32 bit integers"""
    _thrift_type_id = I32

class I16Type(Type):
    """32 bit integers"""
    _thrift_type_id = I16

class I8Type(Type):
    """32 bit integers"""
    _thrift_type_id = I08


class ByteType(Type):
    """Bytes"""
    _thrift_type_id = BYTE

class DoubleType(Type):
    """64 bit doubles"""
    _thrift_type_id = DOUBLE

class UnicodeType(Type):
    """UnicodeType (encoded in binary as utf-8) variable length strings"""
    _thrift_type_id = UTF8

class ByteStringType(Type):
    """Binary variable length strings"""
    _thrift_type_id = STRING

class BooleanType(Type):
    """Boolean values"""
    _thrift_type_id = BOOL

class Field(object):
    def __init__(self, type, tag, initial, optional):
        self.type = type
        self.tag = tag
        self.initial = initial
        self.optional = optional

    def __repr__(self):
        return 'Field(%r, %r, %r, %r)' % (self.type, self.tag, self.initial, self.optional)

class StructType(type):
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
        self.__fields[name] = field

    def fields(self):
        return self.__fields

    def __repr__(self):
        return '<%s.%s fields:%r at 0x%x>' % (self.__module__, self.__name__, self.fields(), id(self))

class Struct(Type):
    __metaclass__ = StructType
    _thrift_type_id = STRUCT

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
    _thrift_type_id = STRUCT

class Map(Type):
    _thrift_type_id = MAP

    key_type = None
    value_type = None

class List(Type):
    _thrift_type_id = LIST

    value_type = None

class Set(Type):
    _thrift_type_id = SET

    value_type = None

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
                field_type = type(set_type_name, (Set, ), {'value_type': context[value_type_name]})
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
    'i16': I16Type,
    'i32': I32Type,
    'i64': I64Type,
    'string': UnicodeType,
    'binary': ByteStringType,
    'double': DoubleType,
    'bool': BooleanType,
}

if __name__ == '__main__':
    import optparse
    import pprint

    parser = optparse.OptionParser()
    parser.add_option('--format', default='json', choices=['yaml', 'json'])
    
    opts, args = parser.parse_args() 
    buf = sys.stdin.read()
    if opts.format == 'json':
        the_types = types_from_json(buf)
    else:
        the_types = types_from_yaml(buf)
    pprint.pprint(the_types['Work'])
