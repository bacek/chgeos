# chgeos Key Pitfalls

- **`system.webassembly_functions` doesn't exist** — use `system.functions WHERE origin != 'System'`
- **WASM can't link natively** — linker errors in `build/` for the WASM target are expected; only `chgeos_tests` links
- **`vector<bool>` specialization** — don't iterate `const auto& v : vec<bool>`, use `T v : vec<T>` (bit_reference issue on Apple Clang)
- **LSP errors** — GEOS headers not in LSP include path; all GEOS-related errors in LSP are false positives
- **st_dwithin bbox check**: the const-col path uses `bbox_a.intersects(wkb_bbox(span_b).expanded(dist))`
- **`early_ret=true` for st_disjoint** — bbox miss means bboxes don't intersect → geometries are disjoint → result is `true`
- **`prep_b_st_containsproperly = nullptr`** — GEOS PreparedGeometry has no B-const acceleration for containsProperly
