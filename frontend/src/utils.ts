import { AdditionalRows, Filter } from "./aside/types"
import { Filters } from "./utils/NestedKVStoreReducer"
import { Rows } from "./aside/AddRow"
import { Template } from "./api/types"

export const hasValue = <T>(it: T | null | undefined): boolean =>
    it !== undefined && it !== null

export const fancyZip = <T>(rows: T[][]): T[][] =>
    rows[0].map((_, c) => rows.map((row) => row[c]))

export const mapFilters = (f: Filters): Filter[][] =>
    Object.values(f).map((ands) =>
        Object.values(ands).filter(
            (it) => hasValue(it.op) && hasValue(it.field),
        ),
    )
export const mapRows = (r: Rows): Record<string, string>[] => {
    return Object.values(r).map((rows) => {
        const fields: Record<string, string> = {}
        Object.values(rows)
            .filter((it) => hasValue(it.field) && hasValue(it.value))
            .forEach(({ field, value }) => {
                fields[field] = value
            })
        return fields
    })
}

export const buildAdditionalRowsFromTemplate = (
    additionalRows: Template["additionalRows"],
): AdditionalRows<Rows> => {
    const build: Rows = {}
    additionalRows.rows.forEach((innerRows, index) => {
        const inner: Rows[number] = {}
        for (const key in innerRows) {
            inner[index] = {
                field: key,
                value: innerRows[key],
            }
        }
        build[index] = inner
    })
    return {
        prepare: additionalRows.prepare,
        rows: build,
    }
}
