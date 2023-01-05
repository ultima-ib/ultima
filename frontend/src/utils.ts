import { Filter } from "./aside/types"
import { Filters } from "./utils/NestedKVStoreReducer"
import { Row, Rows } from "./aside/AddRow"

export const hasValue = <T>(it: T | null | undefined): boolean =>
    it !== undefined && it !== null

export const fancyZip = <T>(rows: T[][]): T[][] =>
    rows[0].map((_, c) => rows.map((row) => row[c]))

export const mapFilters = (f: Filters): Filter[][] =>
    Object.values(f).map((ands) =>
        Object.values(ands).filter(
            (it) => hasValue(it.value) && hasValue(it.op) && hasValue(it.field),
        ),
    )
export const mapRows = (r: Rows): Row[][] =>
    Object.values(r).map((rows) =>
        Object.values(rows).filter(
            (it) => hasValue(it.key) && hasValue(it.value),
        ),
    )
