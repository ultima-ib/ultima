export interface DataSet {
  fields: string[]
  measures: string[]
  groupby: string[]
  overwrites: string[]
  measuresSelected: string[]
  calcParams: CalcParam[],
}

export interface Filter {
  field: string
  op: string
  val: string
}

export interface CalcParam {
  name: string,
  defaultValue?: string
  helperText?: string
}
