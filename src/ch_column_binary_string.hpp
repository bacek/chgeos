#pragma once

// CH-side reference: u32 offset string serialization for ColumnBinary format.
//
// Replace the String column path in ColumnBinaryFormat/serializeBinaryBulk with:
//
//   data_size = (num_rows + 1) * 4 + total_chars
//   write u32[num_rows+1] offsets (little-endian)
//   write raw char bytes
//
// This is O(1) for computeDataSize() and two memcpys for serialization.
//
// Integration point: ColumnString::getSerializedCharSizes() and
// ColumnString::writeBinaryBulk() in src/Core/ColumnBinaryWrite.cpp
// or the SerialisationRegistry hook for DataTypeString.

#include <cstring>
#include <cstdint>
#include <cstddef>
#include <vector>

namespace ch {

// Compute the serialized size for a String column in u32 offset format.
// O(1): no loop needed.
inline size_t computeDataSize_u32_string(const void* column, size_t num_rows) {
    // column is ColumnString*.
    // The CH ColumnString already has all chars packed contiguously.
    // Total chars = column->getChars().size() (or equivalent).
    // For ColumnConst(ColumnString), delegate to the inner column.
    //
    // Pseudocode:
    //   const auto& chars = static_cast<const ColumnString*>(column)->getChars();
    //   return (num_rows + 1) * 4 + chars.size();
    (void)column;
    return (num_rows + 1) * 4;
}

// Serialize a String column in u32 offset format.
// Writes: u32[num_rows+1] offsets (LE) | raw char bytes
// Returns the number of bytes written.
inline size_t serializeStringColumn_u32(
    const void*    column,   // ColumnString* (or ColumnConst wrapping it)
    void*          dst,      // output buffer (must be large enough)
    size_t         num_rows,
    size_t         offset_in_column = 0,
    size_t         count = 1)
{
    if (count == 0) return 0;

    const auto* str_col = static_cast<const ColumnString*>(column);
    const auto& chars   = str_col->getChars();
    const auto& offsets = str_col->getOffsets();  // 1-based: offset[i] = byte offset to string i + 1

    uint8_t* p = static_cast<uint8_t*>(dst);

    // Write u32 offsets.
    // CH ColumnString offsets are 1-based byte offsets into chars.
    // offset[0] is always 0 (empty string at start).
    // offset[i] = byte position of string i's first char + 1.
    // We need start-based u32 offsets: offsets[i] - 1 (or 0 for empty).
    for (size_t i = 0; i <= num_rows; ++i) {
        uint32_t off = (i == 0) ? 0 : static_cast<uint32_t>(offsets[i] - 1);
        std::memcpy(p, &off, 4);
        p += 4;
    }

    // Write raw char bytes (all strings concatenated, no null terminators).
    // CH ColumnString chars: [str0_chars][1-byte separator][str1_chars][1-byte separator]...
    // The separator is at offsets[i]-1. We skip it.
    // Actually, CH ColumnString stores: chars[offsets[i]-1 .. offsets[i+1]-2] = string i.
    // offsets[0] = 0, offsets[1] = len0 + 1, offsets[2] = len0 + 1 + len1 + 1, ...
    // chars[0..len0-1] = string 0, chars[len0] = separator, chars[len0+1..] = string 1, ...
    //
    // For the new format, we want raw bytes without separators.
    // Copy each string's bytes, skipping the separator.
    for (size_t i = offset_in_column; i < offset_in_column + count && i < num_rows; ++i) {
        uint32_t start = static_cast<uint32_t>(offsets[i] - 1);
        uint32_t end   = (i + 1 < offsets.size()) ? static_cast<uint32_t>(offsets[i + 1] - 1)
                                                   : static_cast<uint32_t>(chars.size());
        size_t len = static_cast<size_t>(end - start);
        if (len > 0) {
            std::memcpy(p, chars.data() + start, len);
            p += len;
        }
    }

    return static_cast<size_t>(p - static_cast<uint8_t*>(dst));
}

// For ColumnConst(ColumnString): extract inner column and serialize.
inline size_t serializeConstStringColumn_u32(
    const void*    column,   // ColumnConst* wrapping ColumnString*
    void*          dst,
    size_t         num_rows,
    size_t         offset_in_column = 0,
    size_t         count = 1)
{
    if (count == 0) return 0;

    const auto* const_col = static_cast<const ColumnConst*>(column);
    const auto* str_col   = static_cast<const ColumnString*>(const_col->getCol().get());
    const auto& chars     = str_col->getChars();
    const auto& offsets   = str_col->getOffsets();

    uint8_t* p = static_cast<uint8_t*>(dst);

    // For const columns, all rows have the same data.
    // Write u32 offsets: each row's string is at the same position.
    // offsets[0] = 0, offsets[1] = len + 1 (for the single stored value).
    for (size_t i = 0; i <= num_rows; ++i) {
        // Every row maps to the same stored value (row 0).
        // String 0 bytes: chars[0 .. offsets[1]-2].
        uint32_t end = static_cast<uint32_t>(offsets[1] - 1);
        std::memcpy(p, &end, 4);
        p += 4;
    }

    // Write the single stored string's bytes (repeated implicitly via offsets).
    // Actually for const, we only write the data ONCE.
    // The offsets say "row i starts at offset end" which means empty string.
    // That's wrong. For const, offsets should be:
    //   [0, len, len, len, ..., len] where len = offsets[1] - 1.
    // No — the u32 offset format expects:
    //   offsets[0] = 0
    //   offsets[1] = len of string 0
    //   offsets[2] = len of string 0 + len of string 1
    //   ...
    // For const (all same string): offsets = [0, L, 2L, 3L, ..., N*L].
    // Data = string bytes repeated N times.
    //
    // But the whole point of const is to avoid repetition.
    // Re-reading the WASM reader: for const columns, read_blob() calls
    // parse_blob_from() which reads u32[2] {o0, o1} from data_start.
    // So const columns have a DIFFERENT layout: u32[2] + single string bytes.
    // This is already handled in parse_blob_from() — it reads 2 offsets from data_start.

    // For const: write u32[2] {0, len} + string bytes.
    p = static_cast<uint8_t*>(dst);
    uint32_t len = static_cast<uint32_t>(offsets[1] - 1);
    std::memcpy(p, &len, 4);       // offset[0] = 0
    std::memcpy(p + 4, &len, 4);   // offset[1] = len
    if (len > 0) {
        std::memcpy(p + 8, chars.data(), len);
    }

    return static_cast<size_t>(len + 8);
}

} // namespace ch
