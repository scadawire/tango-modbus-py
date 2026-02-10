"""
Full integration test for ModbusPy using a pymodbus TCP simulator.

Tests all variable types (DevBoolean, DevLong, DevFloat, DevDouble, DevString)
across holding registers, input registers, coils, and discrete inputs,
with big/little endian and normal/swapped word order.

All conversion and I/O logic is exercised through the real ModbusPy class
via unbound method calls -- no code is duplicated.

Usage:
    python test_modbus.py
"""

import struct
import threading
import time
import sys
import traceback

from pymodbus.server.sync import ModbusTcpServer
from pymodbus.datastore import (
    ModbusSequentialDataBlock,
    ModbusSlaveContext,
    ModbusServerContext,
)
from pymodbus.client.sync import ModbusTcpClient
from tango import CmdArgType, AttrDataFormat

from ModbusPy import ModbusPy


# ===========================================================================
#  Lightweight state carrier -- ModbusPy methods only need these attributes.
# ===========================================================================

class State:
    """Carries only instance state; every method lookup falls through to ModbusPy."""
    _REGISTER_TYPE_MAP = ModbusPy._REGISTER_TYPE_MAP

    def __init__(self, client=None, endian="big", word_order="normal"):
        self.client = client
        self.endian = endian
        self.word_order = word_order
        self.dynamicAttributeModbusLookup = {}

    def __getattr__(self, name):
        attr = getattr(ModbusPy, name, None)
        if attr is not None and callable(attr):
            import functools
            return functools.partial(attr, self)
        raise AttributeError(f"'State' has no attribute '{name}'")


# Thin helpers that call the real ModbusPy methods as unbound functions.

def register_attr(s, name, register_str, var_type,
                  data_format=AttrDataFormat.SCALAR, max_x=0, max_y=0):
    s.dynamicAttributeModbusLookup[name] = {
        "variableType": var_type,
        "register": ModbusPy.parse_register(s, register_str),
        "dataFormat": data_format,
        "max_x": max_x,
        "max_y": max_y,
    }


def read_attr(s, name):
    lookup = s.dynamicAttributeModbusLookup[name]
    raw = ModbusPy.modbusRead(s, name)
    data_format = lookup.get("dataFormat", AttrDataFormat.SCALAR)

    if data_format == AttrDataFormat.SPECTRUM:
        return ModbusPy.bytedata_to_array(s, raw, lookup["variableType"], lookup["max_x"])
    elif data_format == AttrDataFormat.IMAGE:
        return ModbusPy.bytedata_to_image(s, raw, lookup["variableType"], lookup["max_x"], lookup["max_y"])
    else:
        return ModbusPy.bytedata_to_variable(
            s, raw, lookup["variableType"], lookup["register"]["subaddr"],
        )


def write_attr(s, name, value):
    lookup = s.dynamicAttributeModbusLookup[name]
    data_format = lookup.get("dataFormat", AttrDataFormat.SCALAR)

    if data_format == AttrDataFormat.SPECTRUM:
        ModbusPy.modbusWrite(s, name, ModbusPy.array_to_bytedata(s, value, lookup["variableType"]))
    elif data_format == AttrDataFormat.IMAGE:
        flat = [v for row in value for v in row]
        ModbusPy.modbusWrite(s, name, ModbusPy.array_to_bytedata(s, flat, lookup["variableType"]))
    elif lookup["variableType"] == CmdArgType.DevBoolean and lookup["register"]["rtype"] == "holding":
        ModbusPy.modbusWriteBooleanBit(s, name, value)
    else:
        ModbusPy.modbusWrite(
            s, name,
            ModbusPy.variable_to_bytedata(
                s, value, lookup["variableType"], lookup["register"]["subaddr"],
            ),
        )


# ===========================================================================
#  Simulator
# ===========================================================================

SIM_PORT = 15502

_server_instance = None


def create_simulator_context():
    store = ModbusSlaveContext(
        hr=ModbusSequentialDataBlock(0, [0] * 500),
        ir=ModbusSequentialDataBlock(0, [0] * 500),
        co=ModbusSequentialDataBlock(0, [0] * 200),
        di=ModbusSequentialDataBlock(0, [0] * 200),
    )
    return ModbusServerContext(slaves=store, single=True)


def run_simulator(context):
    global _server_instance
    _server_instance = ModbusTcpServer(context, address=("127.0.0.1", SIM_PORT))
    _server_instance.serve_forever()


def stop_simulator():
    global _server_instance
    if _server_instance:
        _server_instance.shutdown()
        _server_instance.server_close()
        _server_instance = None


# ===========================================================================
#  Test helpers
# ===========================================================================

passed = 0
failed = 0
errors = []


def assert_equal(test_name, actual, expected, tolerance=None):
    global passed, failed
    if tolerance is not None:
        ok = abs(actual - expected) <= tolerance
    else:
        ok = (actual == expected)

    if ok:
        passed += 1
        print(f"  PASS  {test_name}")
    else:
        failed += 1
        msg = f"  FAIL  {test_name}: expected {expected!r}, got {actual!r}"
        print(msg)
        errors.append(msg)


def assert_list_equal(test_name, actual, expected, tolerance=None):
    global passed, failed
    ok = False
    if len(actual) == len(expected):
        if tolerance is not None:
            ok = all(abs(a - e) <= tolerance for a, e in zip(actual, expected))
        else:
            ok = (actual == expected)

    if ok:
        passed += 1
        print(f"  PASS  {test_name}")
    else:
        failed += 1
        msg = f"  FAIL  {test_name}: expected {expected!r}, got {actual!r}"
        print(msg)
        errors.append(msg)


def assert_2d_equal(test_name, actual, expected, tolerance=None):
    global passed, failed
    ok = False
    if len(actual) == len(expected):
        ok = True
        for row_a, row_e in zip(actual, expected):
            if len(row_a) != len(row_e):
                ok = False
                break
            if tolerance is not None:
                if not all(abs(a - e) <= tolerance for a, e in zip(row_a, row_e)):
                    ok = False
                    break
            else:
                if row_a != row_e:
                    ok = False
                    break

    if ok:
        passed += 1
        print(f"  PASS  {test_name}")
    else:
        failed += 1
        msg = f"  FAIL  {test_name}: expected {expected!r}, got {actual!r}"
        print(msg)
        errors.append(msg)


def assert_true(test_name, value):
    assert_equal(test_name, value, True)


def assert_false(test_name, value):
    assert_equal(test_name, value, False)


# ===========================================================================
#  Test suites
# ===========================================================================

def test_parse_register():
    print("\n-- parse_register --")
    s = State()

    r = ModbusPy.parse_register(s, "1.h.100")
    assert_equal("unit", r["unit"], 1)
    assert_equal("rtype", r["rtype"], "holding")
    assert_equal("addr", r["addr"], 100)
    assert_equal("subaddr", r["subaddr"], 0)

    r = ModbusPy.parse_register(s, "2.i.0x10.5")
    assert_equal("hex addr", r["addr"], 16)
    assert_equal("subaddr present", r["subaddr"], 5)
    assert_equal("input type", r["rtype"], "input")

    r = ModbusPy.parse_register(s, "0.c.0")
    assert_equal("coil type", r["rtype"], "coil")

    r = ModbusPy.parse_register(s, "0.d.0")
    assert_equal("discrete type", r["rtype"], "discrete")

    r = ModbusPy.parse_register(s, "1.holding.50")
    assert_equal("long-form holding", r["rtype"], "holding")


def test_byte_conversions():
    print("\n-- byte conversions (big-endian, normal word order) --")
    s = State(endian="big", word_order="normal")

    # DevFloat
    enc = ModbusPy.variable_to_bytedata(s, 3.14, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevFloat)
    assert_equal("DevFloat round-trip 3.14", dec, 3.14, tolerance=1e-5)

    enc = ModbusPy.variable_to_bytedata(s, -0.5, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevFloat)
    assert_equal("DevFloat round-trip -0.5", dec, -0.5, tolerance=1e-7)

    # DevDouble
    enc = ModbusPy.variable_to_bytedata(s, 2.718281828, CmdArgType.DevDouble)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevDouble)
    assert_equal("DevDouble round-trip 2.718281828", dec, 2.718281828, tolerance=1e-9)

    # DevLong
    enc = ModbusPy.variable_to_bytedata(s, 123456, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevLong)
    assert_equal("DevLong round-trip 123456", dec, 123456)

    enc = ModbusPy.variable_to_bytedata(s, -9999, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevLong)
    assert_equal("DevLong round-trip -9999", dec, -9999)

    # DevBoolean from bytes (register-based)
    enc = ModbusPy.variable_to_bytedata(s, True, CmdArgType.DevBoolean)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevBoolean)
    assert_true("DevBoolean True round-trip", dec)

    enc = ModbusPy.variable_to_bytedata(s, False, CmdArgType.DevBoolean)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevBoolean)
    assert_false("DevBoolean False round-trip", dec)

    # DevBoolean with suboffset (bit 3)
    enc = ModbusPy.variable_to_bytedata(s, True, CmdArgType.DevBoolean, suboffset=3)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevBoolean, suboffset=3)
    assert_true("DevBoolean bit3 True round-trip", dec)

    # DevBoolean from raw bool (coil path)
    dec = ModbusPy.bytedata_to_variable(s, True, CmdArgType.DevBoolean)
    assert_true("DevBoolean from raw True", dec)
    dec = ModbusPy.bytedata_to_variable(s, False, CmdArgType.DevBoolean)
    assert_false("DevBoolean from raw False", dec)

    # DevString
    enc = ModbusPy.variable_to_bytedata(s, "Hello", CmdArgType.DevString, suboffset=20)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevString, suboffset=20)
    assert_equal("DevString round-trip 'Hello'", dec, "Hello")

    enc = ModbusPy.variable_to_bytedata(s, "", CmdArgType.DevString, suboffset=10)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevString, suboffset=10)
    assert_equal("DevString round-trip empty", dec, "")


def test_byte_conversions_little_endian():
    print("\n-- byte conversions (little-endian, normal word order) --")
    s = State(endian="little", word_order="normal")

    enc = ModbusPy.variable_to_bytedata(s, 3.14, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevFloat)
    assert_equal("LE DevFloat 3.14", dec, 3.14, tolerance=1e-5)

    enc = ModbusPy.variable_to_bytedata(s, 42.0, CmdArgType.DevDouble)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevDouble)
    assert_equal("LE DevDouble 42.0", dec, 42.0, tolerance=1e-9)

    enc = ModbusPy.variable_to_bytedata(s, -100, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevLong)
    assert_equal("LE DevLong -100", dec, -100)


def test_byte_conversions_swapped_word_order():
    print("\n-- byte conversions (big-endian, swapped word order) --")
    s = State(endian="big", word_order="swapped")

    enc = ModbusPy.variable_to_bytedata(s, 1.5, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevFloat)
    assert_equal("Swapped DevFloat 1.5", dec, 1.5, tolerance=1e-7)

    enc = ModbusPy.variable_to_bytedata(s, 99.99, CmdArgType.DevDouble)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevDouble)
    assert_equal("Swapped DevDouble 99.99", dec, 99.99, tolerance=1e-9)

    enc = ModbusPy.variable_to_bytedata(s, 0x12345678, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_variable(s, enc, CmdArgType.DevLong)
    assert_equal("Swapped DevLong 0x12345678", dec, 0x12345678)


def test_holding_register_float(s):
    print("\n-- holding register: DevFloat --")
    register_attr(s, "hfloat", "1.h.10", CmdArgType.DevFloat)

    for val in [0.0, 1.0, -1.0, 3.14159, 1e10, -1e-5]:
        write_attr(s, "hfloat", val)
        got = read_attr(s, "hfloat")
        assert_equal(f"holding float {val}", got, val, tolerance=max(abs(val) * 1e-6, 1e-7))


def test_holding_register_double(s):
    print("\n-- holding register: DevDouble --")
    register_attr(s, "hdouble", "1.h.20", CmdArgType.DevDouble)

    for val in [0.0, 2.718281828459045, -1e100, 1e-300]:
        write_attr(s, "hdouble", val)
        got = read_attr(s, "hdouble")
        assert_equal(f"holding double {val}", got, val, tolerance=max(abs(val) * 1e-12, 1e-300))


def test_holding_register_long(s):
    print("\n-- holding register: DevLong --")
    register_attr(s, "hlong", "1.h.30", CmdArgType.DevLong)

    for val in [0, 1, -1, 32767, -32768, 2147483647, -2147483648]:
        write_attr(s, "hlong", val)
        got = read_attr(s, "hlong")
        assert_equal(f"holding long {val}", got, val)


def test_holding_register_boolean(s):
    print("\n-- holding register: DevBoolean --")
    register_attr(s, "hbool", "1.h.40", CmdArgType.DevBoolean)

    write_attr(s, "hbool", True)
    got = read_attr(s, "hbool")
    assert_true("holding bool True", got)

    write_attr(s, "hbool", False)
    got = read_attr(s, "hbool")
    assert_false("holding bool False", got)


def test_holding_register_boolean_suboffset(s):
    print("\n-- holding register: DevBoolean with bit offset --")
    register_attr(s, "hbool_bit3", "1.h.42.3", CmdArgType.DevBoolean)

    write_attr(s, "hbool_bit3", True)
    got = read_attr(s, "hbool_bit3")
    assert_true("holding bool bit3 True", got)

    write_attr(s, "hbool_bit3", False)
    got = read_attr(s, "hbool_bit3")
    assert_false("holding bool bit3 False", got)


def test_holding_register_boolean_multibit(s):
    print("\n-- holding register: DevBoolean multi-bit (same register) --")
    register_attr(s, "hbit0", "1.h.44.0", CmdArgType.DevBoolean)
    register_attr(s, "hbit1", "1.h.44.1", CmdArgType.DevBoolean)
    register_attr(s, "hbit2", "1.h.44.2", CmdArgType.DevBoolean)
    register_attr(s, "hbit3", "1.h.44.3", CmdArgType.DevBoolean)

    # Set all four bits
    write_attr(s, "hbit0", True)
    write_attr(s, "hbit1", True)
    write_attr(s, "hbit2", True)
    write_attr(s, "hbit3", True)

    assert_true("multibit bit0 True", read_attr(s, "hbit0"))
    assert_true("multibit bit1 True", read_attr(s, "hbit1"))
    assert_true("multibit bit2 True", read_attr(s, "hbit2"))
    assert_true("multibit bit3 True", read_attr(s, "hbit3"))

    # Clear bit1 only -- bits 0, 2, 3 must remain set
    write_attr(s, "hbit1", False)
    assert_true("multibit bit0 still True", read_attr(s, "hbit0"))
    assert_false("multibit bit1 now False", read_attr(s, "hbit1"))
    assert_true("multibit bit2 still True", read_attr(s, "hbit2"))
    assert_true("multibit bit3 still True", read_attr(s, "hbit3"))

    # Clear all
    write_attr(s, "hbit0", False)
    write_attr(s, "hbit2", False)
    write_attr(s, "hbit3", False)
    assert_false("multibit bit0 cleared", read_attr(s, "hbit0"))
    assert_false("multibit bit1 still cleared", read_attr(s, "hbit1"))
    assert_false("multibit bit2 cleared", read_attr(s, "hbit2"))
    assert_false("multibit bit3 cleared", read_attr(s, "hbit3"))

    # Set alternating pattern
    write_attr(s, "hbit0", True)
    write_attr(s, "hbit2", True)
    assert_true("alternating bit0", read_attr(s, "hbit0"))
    assert_false("alternating bit1", read_attr(s, "hbit1"))
    assert_true("alternating bit2", read_attr(s, "hbit2"))
    assert_false("alternating bit3", read_attr(s, "hbit3"))


def test_holding_register_string(s):
    print("\n-- holding register: DevString --")
    register_attr(s, "hstring", "1.h.50.20", CmdArgType.DevString)

    for val in ["Hello", "Modbus", "Test123", ""]:
        write_attr(s, "hstring", val)
        got = read_attr(s, "hstring")
        assert_equal(f"holding string '{val}'", got, val)


def test_holding_register_string_long(s):
    print("\n-- holding register: DevString (longer) --")
    register_attr(s, "hstring_long", "1.h.70.40", CmdArgType.DevString)

    val = "A longer test string!!"
    write_attr(s, "hstring_long", val)
    got = read_attr(s, "hstring_long")
    assert_equal("holding long string", got, val)


def test_coil_boolean(s):
    print("\n-- coil: DevBoolean --")
    register_attr(s, "coil_bool", "1.c.0", CmdArgType.DevBoolean)

    write_attr(s, "coil_bool", True)
    got = read_attr(s, "coil_bool")
    assert_true("coil True", got)

    write_attr(s, "coil_bool", False)
    got = read_attr(s, "coil_bool")
    assert_false("coil False", got)


def test_coil_multiple(s):
    print("\n-- coil: multiple coils --")
    for i in range(5):
        register_attr(s, f"coil_{i}", f"1.c.{10 + i}", CmdArgType.DevBoolean)

    for i in range(5):
        write_attr(s, f"coil_{i}", bool(i % 2))

    for i in range(5):
        got = read_attr(s, f"coil_{i}")
        expected = bool(i % 2)
        assert_equal(f"coil_{i} = {expected}", got, expected)


def test_input_register_float(s, context):
    print("\n-- input register: DevFloat (pre-seeded) --")
    register_attr(s, "ifloat", "1.i.10", CmdArgType.DevFloat)

    val = 3.14
    raw = struct.pack(">f", val)
    reg0 = struct.unpack(">H", raw[0:2])[0]
    reg1 = struct.unpack(">H", raw[2:4])[0]
    context[0x01].setValues(4, 10, [reg0, reg1])

    got = read_attr(s, "ifloat")
    assert_equal("input float 3.14", got, val, tolerance=1e-5)


def test_input_register_long(s, context):
    print("\n-- input register: DevLong (pre-seeded) --")
    register_attr(s, "ilong", "1.i.20", CmdArgType.DevLong)

    val = -42
    raw = struct.pack(">i", val)
    reg0 = struct.unpack(">H", raw[0:2])[0]
    reg1 = struct.unpack(">H", raw[2:4])[0]
    context[0x01].setValues(4, 20, [reg0, reg1])

    got = read_attr(s, "ilong")
    assert_equal("input long -42", got, val)


def test_input_register_double(s, context):
    print("\n-- input register: DevDouble (pre-seeded) --")
    register_attr(s, "idouble", "1.i.30", CmdArgType.DevDouble)

    val = 1.23456789012345
    raw = struct.pack(">d", val)
    regs = [struct.unpack(">H", raw[i:i+2])[0] for i in range(0, 8, 2)]
    context[0x01].setValues(4, 30, regs)

    got = read_attr(s, "idouble")
    assert_equal("input double 1.23456789012345", got, val, tolerance=1e-12)


def test_input_register_string(s, context):
    print("\n-- input register: DevString (pre-seeded) --")
    register_attr(s, "istring", "1.i.40.16", CmdArgType.DevString)

    val = "sensor"
    raw = val.encode("utf-8")
    raw += b"\x00" * (16 - len(raw))
    regs = [struct.unpack(">H", raw[i:i+2])[0] for i in range(0, len(raw), 2)]
    context[0x01].setValues(4, 40, regs)

    got = read_attr(s, "istring")
    assert_equal("input string 'sensor'", got, val)


def test_discrete_input(s, context):
    print("\n-- discrete input: DevBoolean (pre-seeded) --")
    register_attr(s, "di_bool", "1.d.5", CmdArgType.DevBoolean)

    store = context[0x01]
    store.setValues(2, 5, [1])

    got = read_attr(s, "di_bool")
    assert_true("discrete True", got)

    store.setValues(2, 5, [0])
    got = read_attr(s, "di_bool")
    assert_false("discrete False", got)


def test_swapped_word_order_over_modbus(context):
    print("\n-- holding register: swapped word order over modbus --")
    client = ModbusTcpClient("127.0.0.1", port=SIM_PORT)
    assert client.connect(), "Swapped client failed to connect"
    s = State(client, endian="big", word_order="swapped")

    register_attr(s, "sw_float", "1.h.100", CmdArgType.DevFloat)
    register_attr(s, "sw_double", "1.h.110", CmdArgType.DevDouble)
    register_attr(s, "sw_long", "1.h.120", CmdArgType.DevLong)

    val_f = -7.5
    write_attr(s, "sw_float", val_f)
    got = read_attr(s, "sw_float")
    assert_equal(f"swapped float {val_f}", got, val_f, tolerance=1e-6)

    val_d = 123456.789012
    write_attr(s, "sw_double", val_d)
    got = read_attr(s, "sw_double")
    assert_equal(f"swapped double {val_d}", got, val_d, tolerance=1e-6)

    val_l = -123456
    write_attr(s, "sw_long", val_l)
    got = read_attr(s, "sw_long")
    assert_equal(f"swapped long {val_l}", got, val_l)

    client.close()


def test_little_endian_over_modbus(context):
    print("\n-- holding register: little-endian over modbus --")
    client = ModbusTcpClient("127.0.0.1", port=SIM_PORT)
    assert client.connect(), "LE client failed to connect"
    s = State(client, endian="little", word_order="normal")

    register_attr(s, "le_float", "1.h.130", CmdArgType.DevFloat)
    register_attr(s, "le_long", "1.h.140", CmdArgType.DevLong)

    val_f = 9.81
    write_attr(s, "le_float", val_f)
    got = read_attr(s, "le_float")
    assert_equal(f"LE modbus float {val_f}", got, val_f, tolerance=1e-5)

    val_l = 65536
    write_attr(s, "le_long", val_l)
    got = read_attr(s, "le_long")
    assert_equal(f"LE modbus long {val_l}", got, val_l)

    client.close()


# ===========================================================================
#  Spectrum (1D) tests
# ===========================================================================

def test_spectrum_float_conversion():
    print("\n-- spectrum conversion: DevFloat --")
    s = State(endian="big", word_order="normal")

    values = [1.5, -2.5, 3.14]
    enc = ModbusPy.array_to_bytedata(s, values, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevFloat, 3)
    assert_list_equal("spectrum float encode/decode", dec, values, tolerance=1e-5)


def test_spectrum_double_conversion():
    print("\n-- spectrum conversion: DevDouble --")
    s = State(endian="big", word_order="normal")

    values = [1.111, 2.222, 3.333]
    enc = ModbusPy.array_to_bytedata(s, values, CmdArgType.DevDouble)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevDouble, 3)
    assert_list_equal("spectrum double encode/decode", dec, values, tolerance=1e-9)


def test_spectrum_long_conversion():
    print("\n-- spectrum conversion: DevLong --")
    s = State(endian="big", word_order="normal")

    values = [100, -200, 300, 0]
    enc = ModbusPy.array_to_bytedata(s, values, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevLong, 4)
    assert_list_equal("spectrum long encode/decode", dec, values)


def test_spectrum_boolean_conversion():
    print("\n-- spectrum conversion: DevBoolean (bit-packed) --")
    s = State(endian="big", word_order="normal")

    # 4 bools should pack into 1 register (bits 0-3)
    values = [True, False, True, False]
    enc = ModbusPy.array_to_bytedata(s, values, CmdArgType.DevBoolean)
    assert_equal("4 bools pack to 2 bytes", len(enc), 2)
    # bit pattern: bit0=1, bit1=0, bit2=1, bit3=0 → 0b0101 = 5
    assert_equal("4 bools packed value", struct.unpack(">H", enc)[0], 0x0005)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevBoolean, 4)
    assert_list_equal("spectrum bool 4 round-trip", dec, values)

    # 16 bools = exactly 1 register
    values_16 = [bool(i % 3 == 0) for i in range(16)]
    enc = ModbusPy.array_to_bytedata(s, values_16, CmdArgType.DevBoolean)
    assert_equal("16 bools pack to 2 bytes", len(enc), 2)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevBoolean, 16)
    assert_list_equal("spectrum bool 16 round-trip", dec, values_16)

    # 20 bools = 2 registers (16 + 4 bits)
    values_20 = [bool(i % 2 == 0) for i in range(20)]
    enc = ModbusPy.array_to_bytedata(s, values_20, CmdArgType.DevBoolean)
    assert_equal("20 bools pack to 4 bytes", len(enc), 4)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevBoolean, 20)
    assert_list_equal("spectrum bool 20 round-trip", dec, values_20)

    # all True
    values_all = [True] * 16
    enc = ModbusPy.array_to_bytedata(s, values_all, CmdArgType.DevBoolean)
    assert_equal("16 all-true packed", struct.unpack(">H", enc)[0], 0xFFFF)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevBoolean, 16)
    assert_list_equal("spectrum bool all-true round-trip", dec, values_all)

    # all False
    values_none = [False] * 16
    enc = ModbusPy.array_to_bytedata(s, values_none, CmdArgType.DevBoolean)
    assert_equal("16 all-false packed", struct.unpack(">H", enc)[0], 0x0000)
    dec = ModbusPy.bytedata_to_array(s, enc, CmdArgType.DevBoolean, 16)
    assert_list_equal("spectrum bool all-false round-trip", dec, values_none)


def test_spectrum_holding_float(s):
    print("\n-- spectrum holding: DevFloat --")
    # 5 floats starting at register 200 (5*2 = 10 registers)
    register_attr(s, "sp_hfloat", "1.h.200", CmdArgType.DevFloat,
                  AttrDataFormat.SPECTRUM, max_x=5)

    values = [1.0, -2.5, 3.14, 0.0, 100.0]
    write_attr(s, "sp_hfloat", values)
    got = read_attr(s, "sp_hfloat")
    assert_list_equal("spectrum holding float", got, values, tolerance=1e-5)


def test_spectrum_holding_double(s):
    print("\n-- spectrum holding: DevDouble --")
    # 3 doubles starting at register 220 (3*4 = 12 registers)
    register_attr(s, "sp_hdouble", "1.h.220", CmdArgType.DevDouble,
                  AttrDataFormat.SPECTRUM, max_x=3)

    values = [1.111, -2.222, 3.333]
    write_attr(s, "sp_hdouble", values)
    got = read_attr(s, "sp_hdouble")
    assert_list_equal("spectrum holding double", got, values, tolerance=1e-9)


def test_spectrum_holding_long(s):
    print("\n-- spectrum holding: DevLong --")
    # 4 longs starting at register 240 (4*2 = 8 registers)
    register_attr(s, "sp_hlong", "1.h.240", CmdArgType.DevLong,
                  AttrDataFormat.SPECTRUM, max_x=4)

    values = [0, 1, -1, 2147483647]
    write_attr(s, "sp_hlong", values)
    got = read_attr(s, "sp_hlong")
    assert_list_equal("spectrum holding long", got, values)


def test_spectrum_holding_boolean(s):
    print("\n-- spectrum holding: DevBoolean (bit-packed) --")
    # 4 booleans packed into 1 register at address 260
    register_attr(s, "sp_hbool", "1.h.260", CmdArgType.DevBoolean,
                  AttrDataFormat.SPECTRUM, max_x=4)

    values = [True, False, True, False]
    write_attr(s, "sp_hbool", values)
    got = read_attr(s, "sp_hbool")
    assert_list_equal("spectrum holding bool 4-bit", got, values)

    # 16 booleans = exactly 1 register at address 262
    register_attr(s, "sp_hbool16", "1.h.262", CmdArgType.DevBoolean,
                  AttrDataFormat.SPECTRUM, max_x=16)

    values_16 = [True, False] * 8
    write_attr(s, "sp_hbool16", values_16)
    got = read_attr(s, "sp_hbool16")
    assert_list_equal("spectrum holding bool 16-bit", got, values_16)

    # 20 booleans = 2 registers at address 264
    register_attr(s, "sp_hbool20", "1.h.264", CmdArgType.DevBoolean,
                  AttrDataFormat.SPECTRUM, max_x=20)

    values_20 = [bool(i % 3 == 0) for i in range(20)]
    write_attr(s, "sp_hbool20", values_20)
    got = read_attr(s, "sp_hbool20")
    assert_list_equal("spectrum holding bool 20-bit cross-register", got, values_20)


def test_spectrum_input_float(s, context):
    print("\n-- spectrum input: DevFloat (pre-seeded) --")
    register_attr(s, "sp_ifloat", "1.i.100", CmdArgType.DevFloat,
                  AttrDataFormat.SPECTRUM, max_x=3)

    values = [10.0, 20.0, 30.0]
    raw = b""
    for v in values:
        raw += struct.pack(">f", v)
    regs = [struct.unpack(">H", raw[i:i+2])[0] for i in range(0, len(raw), 2)]
    context[0x01].setValues(4, 100, regs)

    got = read_attr(s, "sp_ifloat")
    assert_list_equal("spectrum input float", got, values, tolerance=1e-5)


# ===========================================================================
#  Image (2D) tests
# ===========================================================================

def test_image_float_conversion():
    print("\n-- image conversion: DevFloat --")
    s = State(endian="big", word_order="normal")

    data = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
    flat = [v for row in data for v in row]
    enc = ModbusPy.array_to_bytedata(s, flat, CmdArgType.DevFloat)
    dec = ModbusPy.bytedata_to_image(s, enc, CmdArgType.DevFloat, 3, 2)
    assert_2d_equal("image float encode/decode", dec, data, tolerance=1e-5)


def test_image_long_conversion():
    print("\n-- image conversion: DevLong --")
    s = State(endian="big", word_order="normal")

    data = [[1, 2], [3, 4], [5, 6]]
    flat = [v for row in data for v in row]
    enc = ModbusPy.array_to_bytedata(s, flat, CmdArgType.DevLong)
    dec = ModbusPy.bytedata_to_image(s, enc, CmdArgType.DevLong, 2, 3)
    assert_2d_equal("image long encode/decode", dec, data)


def test_image_holding_float(s):
    print("\n-- image holding: DevFloat --")
    # 2x3 floats starting at register 300 (6*2 = 12 registers)
    register_attr(s, "img_hfloat", "1.h.300", CmdArgType.DevFloat,
                  AttrDataFormat.IMAGE, max_x=3, max_y=2)

    data = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
    write_attr(s, "img_hfloat", data)
    got = read_attr(s, "img_hfloat")
    assert_2d_equal("image holding float", got, data, tolerance=1e-5)


def test_image_holding_double(s):
    print("\n-- image holding: DevDouble --")
    # 2x2 doubles starting at register 320 (4*4 = 16 registers)
    register_attr(s, "img_hdouble", "1.h.320", CmdArgType.DevDouble,
                  AttrDataFormat.IMAGE, max_x=2, max_y=2)

    data = [[1.1, 2.2], [3.3, 4.4]]
    write_attr(s, "img_hdouble", data)
    got = read_attr(s, "img_hdouble")
    assert_2d_equal("image holding double", got, data, tolerance=1e-9)


def test_image_holding_long(s):
    print("\n-- image holding: DevLong --")
    # 3x2 longs starting at register 350 (6*2 = 12 registers)
    register_attr(s, "img_hlong", "1.h.350", CmdArgType.DevLong,
                  AttrDataFormat.IMAGE, max_x=2, max_y=3)

    data = [[10, 20], [30, 40], [50, 60]]
    write_attr(s, "img_hlong", data)
    got = read_attr(s, "img_hlong")
    assert_2d_equal("image holding long", got, data)


def test_image_input_float(s, context):
    print("\n-- image input: DevFloat (pre-seeded) --")
    register_attr(s, "img_ifloat", "1.i.150", CmdArgType.DevFloat,
                  AttrDataFormat.IMAGE, max_x=2, max_y=2)

    data = [[1.5, 2.5], [3.5, 4.5]]
    raw = b""
    for row in data:
        for v in row:
            raw += struct.pack(">f", v)
    regs = [struct.unpack(">H", raw[i:i+2])[0] for i in range(0, len(raw), 2)]
    context[0x01].setValues(4, 150, regs)

    got = read_attr(s, "img_ifloat")
    assert_2d_equal("image input float", got, data, tolerance=1e-5)


def test_edge_cases(s):
    print("\n-- edge cases --")
    import math

    register_attr(s, "edge_float", "1.h.150", CmdArgType.DevFloat)

    write_attr(s, "edge_float", float("inf"))
    got = read_attr(s, "edge_float")
    assert_true("float +inf", math.isinf(got) and got > 0)

    write_attr(s, "edge_float", float("-inf"))
    got = read_attr(s, "edge_float")
    assert_true("float -inf", math.isinf(got) and got < 0)

    write_attr(s, "edge_float", float("nan"))
    got = read_attr(s, "edge_float")
    assert_true("float nan", math.isnan(got))

    register_attr(s, "edge_long", "1.h.160", CmdArgType.DevLong)
    write_attr(s, "edge_long", 0)
    got = read_attr(s, "edge_long")
    assert_equal("long zero", got, 0)

    register_attr(s, "edge_str", "1.h.170.10", CmdArgType.DevString)
    val = "123456789"  # 9 chars + 1 null = 10 bytes
    write_attr(s, "edge_str", val)
    got = read_attr(s, "edge_str")
    assert_equal("string near-max", got, val)


# ===========================================================================
#  Main
# ===========================================================================

def main():
    global passed, failed

    # -- pure conversion tests (no server needed) --
    test_parse_register()
    test_byte_conversions()
    test_byte_conversions_little_endian()
    test_byte_conversions_swapped_word_order()

    # -- spectrum/image conversion tests (no server needed) --
    test_spectrum_float_conversion()
    test_spectrum_double_conversion()
    test_spectrum_long_conversion()
    test_spectrum_boolean_conversion()
    test_image_float_conversion()
    test_image_long_conversion()

    # -- start simulator --
    print("\n== Starting Modbus TCP simulator on port", SIM_PORT, "==")
    context = create_simulator_context()
    server_thread = threading.Thread(target=run_simulator, args=(context,), daemon=True)
    server_thread.start()
    time.sleep(0.5)

    try:
        client = ModbusTcpClient("127.0.0.1", port=SIM_PORT)
        if not client.connect():
            print("FATAL: Cannot connect to simulator")
            sys.exit(1)
        print("Connected to simulator\n")

        s = State(client, endian="big", word_order="normal")

        # -- holding register tests --
        test_holding_register_float(s)
        test_holding_register_double(s)
        test_holding_register_long(s)
        test_holding_register_boolean(s)
        test_holding_register_boolean_suboffset(s)
        test_holding_register_boolean_multibit(s)
        test_holding_register_string(s)
        test_holding_register_string_long(s)

        # -- coil tests --
        test_coil_boolean(s)
        test_coil_multiple(s)

        # -- input register tests (read-only, pre-seeded) --
        test_input_register_float(s, context)
        test_input_register_long(s, context)
        test_input_register_double(s, context)
        test_input_register_string(s, context)

        # -- discrete input tests --
        test_discrete_input(s, context)

        # -- spectrum holding register tests --
        test_spectrum_holding_float(s)
        test_spectrum_holding_double(s)
        test_spectrum_holding_long(s)
        test_spectrum_holding_boolean(s)

        # -- spectrum input register tests --
        test_spectrum_input_float(s, context)

        # -- image holding register tests --
        test_image_holding_float(s)
        test_image_holding_double(s)
        test_image_holding_long(s)

        # -- image input register tests --
        test_image_input_float(s, context)

        # -- endian / word order variants over modbus --
        test_swapped_word_order_over_modbus(context)
        test_little_endian_over_modbus(context)

        # -- edge cases --
        test_edge_cases(s)

        client.close()

    except Exception:
        traceback.print_exc()
        failed += 1
    finally:
        stop_simulator()

    # -- summary --
    total = passed + failed
    print(f"\n{'=' * 50}")
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    if errors:
        print("\n  Failures:")
        for e in errors:
            print(f"    {e}")
    print(f"{'=' * 50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
