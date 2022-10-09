import {Filter} from "./aside/types";
import {Filters} from "./aside/filters/reducer";

const hasValue = <T>(it: T | null | undefined): boolean => it !== undefined && it !== null

export const fancyZip = <T>(rows: T[][]): T[][] => rows[0].map((_, c) => rows.map(row => row[c]))
export const mapFilters = (f: Filters): Filter[][] => Object.values(f)
    .map((it) => (Object.values(it) as Filter[]).filter(it => hasValue(it.value) && hasValue(it.op) && hasValue(it.field)))
