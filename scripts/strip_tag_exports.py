#!/usr/bin/env python3
"""
Strip exception tag exports from a WASM binary.

Emscripten exports __cpp_exception as a tag when -fwasm-exceptions is used.
Wasmtime's C API panics on ExternType::Tag (issue #10252), so we remove
tag exports from the binary post-build.

Usage: strip_tag_exports.py <input.wasm> [output.wasm]
If output is omitted the file is modified in place.
"""

import sys
import struct

def read_leb128(data, offset):
    result, shift = 0, 0
    while True:
        b = data[offset]; offset += 1
        result |= (b & 0x7f) << shift
        shift += 7
        if not (b & 0x80):
            break
    return result, offset

def write_leb128(value):
    out = []
    while True:
        b = value & 0x7f
        value >>= 7
        if value:
            b |= 0x80
        out.append(b)
        if not value:
            break
    return bytes(out)

WASM_MAGIC    = b'\x00asm'
SECTION_EXPORT = 7
EXTERN_TAG     = 4  # kind byte for exception tags

def strip_tag_exports(data):
    assert data[:4] == WASM_MAGIC, "not a wasm file"
    out = bytearray(data[:8])  # magic + version
    offset = 8

    while offset < len(data):
        section_id = data[offset]; offset += 1
        section_size, offset = read_leb128(data, offset)
        section_data = data[offset:offset + section_size]
        offset += section_size

        if section_id != SECTION_EXPORT:
            out += bytes([section_id])
            out += write_leb128(section_size)
            out += section_data
            continue

        # Parse and rewrite exports section, dropping tag entries.
        pos = 0
        count, pos = read_leb128(section_data, pos)
        entries = []
        for _ in range(count):
            name_len, pos = read_leb128(section_data, pos)
            name = section_data[pos:pos + name_len]; pos += name_len
            kind = section_data[pos]; pos += 1
            idx, pos = read_leb128(section_data, pos)
            if kind == EXTERN_TAG:
                print(f"  stripped tag export: {name.decode()}", file=sys.stderr)
                continue
            entries.append((name, kind, idx))

        new_section = bytearray()
        new_section += write_leb128(len(entries))
        for name, kind, idx in entries:
            new_section += write_leb128(len(name))
            new_section += name
            new_section += bytes([kind])
            new_section += write_leb128(idx)

        out += bytes([section_id])
        out += write_leb128(len(new_section))
        out += new_section

    return bytes(out)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <input.wasm> [output.wasm]", file=sys.stderr)
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else input_path

    data = open(input_path, "rb").read()
    result = strip_tag_exports(data)
    open(output_path, "wb").write(result)
    print(f"wrote {output_path} ({len(result)} bytes)", file=sys.stderr)
