import { Filter, Override } from "../aside/types"
import { Row } from "../aside/AddRow"

export interface GenerateTableDataRequest {
    measures: [string, string][]
    calc_params: Record<string, string>
    filters: Filter[][]
    groupby: string[]
    overrides: Override[]
    hide_zeros: boolean
    totals: boolean
    additionalRows: Row[][]
}

export interface GenerateTableDataResponse {
    columns: {
        name: string
        datatype: string
        values: (string | number | null)[]
    }[]
}

export interface Template extends GenerateTableDataRequest {
    name: string
}
