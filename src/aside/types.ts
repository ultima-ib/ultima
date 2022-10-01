export interface DataSet {
  fields: string[]
  measures: string[]
  groupby: string[]
  overwrites: string[]
  measuresSelected: string[]
}

export interface Filter {
  field: string
  op: string
  val: string
}
