import { Filter } from "./aside/types"
import { Filters } from "./aside/filters/reducer"

const hasValue = <T>(it: T | null | undefined): boolean =>
    it !== undefined && it !== null

export const fancyZip = <T>(rows: T[][]): T[][] =>
    rows[0].map((_, c) => rows.map((row) => row[c]))
export const mapFilters = (f: Filters): Filter[][] =>
    Object.values(f).map((ands) =>
        Object.values(ands).filter(
            (it) => hasValue(it.value) && hasValue(it.op) && hasValue(it.field),
        ),
    )

export const fancyFilter = <T>(
    array: T[],
    predicate: (it: T) => boolean,
): [T[], T[]] => {
    const unmatched: T[] = []
    const matched = array.filter((it) => {
        if (predicate(it)) {
            return true
        } else {
            unmatched.push(it)
            return false
        }
    })
    return [matched, unmatched]
}
