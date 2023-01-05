import { Filter, Override } from "../aside/types"

export interface GenerateTableDataRequest {
    measures: [string, string][]
    calc_params: Record<string, string>
    filters: Filter[][]
    groupby: string[]
    overrides: Override[]
    hide_zeros: boolean
    totals: boolean
    additionalRows: Record<string, string>[]
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
