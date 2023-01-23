export interface DataSet {
    fields: string[]
    measures: string[]
    groupby: string[]
    measuresSelected: string[]
    calcParams: CalcParam[]
}

export interface Filter {
    field?: string
    op?: string
    value?: string | string[]
}

export interface CalcParam {
    name: string
    defaultValue: string | null
    helperText?: string
}

export interface Override {
    field?: string
    value?: string
    filters: Filter[][]
}

export interface AdditionalRows<R = Record<string, string>[]> {
    prepare: boolean
    rows: R
}
