import { createContext, useContext } from "react"
import { DataSet, Filter, Filter as FilterType, Override } from "./types"
import type { Template } from "../api/types"
import { Filters } from "./filters/reducer"
import { GenerateTableDataRequest } from "../api/types"
import { mapFilters } from "../utils"

export enum InputStateUpdate {
    // eslint-disable-next-line
    DataSet,
    // eslint-disable-next-line
    Filters,
    HideZeros,
    Total,
    AggData,
    Overrides,
    TemplateSelect,
    CalcParamUpdate,
}

type Data = Partial<Omit<InputStateContext, "dispatcher">>

export function inputStateReducer(
    state: InputStateContext,
    action: { type: InputStateUpdate; data: Data },
): InputStateContext {
    let update: Data
    switch (action.type) {
        case InputStateUpdate.DataSet:
            update = {
                dataSet: {
                    ...state.dataSet,
                    ...action.data.dataSet,
                },
            }
            break
        case InputStateUpdate.Filters:
            update = {
                filters: {
                    ...action.data.filters,
                },
            }
            break
        case InputStateUpdate.HideZeros:
            update = {
                hideZeros: action.data.hideZeros,
            }
            break
        case InputStateUpdate.Total:
            update = {
                totals: action.data.totals,
            }
            break
        case InputStateUpdate.AggData:
            update = {
                aggData: {
                    ...state.aggData,
                    ...action.data.aggData,
                },
            }
            break
        case InputStateUpdate.Overrides: {
            update = {
                overrides: {
                    ...state.overrides,
                    ...action.data.overrides,
                },
            }
            break
        }
        case InputStateUpdate.TemplateSelect: {
            const data = action.data as Template
            const dataSet = {
                ...state.dataSet,
                groupby: data.groupby,
                measuresSelected: data.measures.map((it) => it[0]),
            }

            const buildFilters = (filters: FilterType[][]): Filters => {
                const build: Filters = {}
                filters.forEach((newFilters, index) => {
                    const inner: Record<number, Filter> = {}
                    newFilters.forEach((newFilter, idx) => {
                        inner[idx] = newFilter
                    })
                    build[index] = inner
                })
                return build
            }
            data.overrides.map((override) => ({
                value: override.value,
                field: override.field,
                filters: buildFilters(override.filters),
            }))

            const aggData: Record<string, string> = {}
            data.measures.forEach(([key, value]) => {
                aggData[key] = value
            })
            update = {
                dataSet,
                filters: buildFilters(data.filters),
                hideZeros: data.hide_zeros,
                totals: data.totals,
                calcParams: data.calc_params,
                overrides: data.overrides,
                aggData,
            }
            break
        }
        case InputStateUpdate.CalcParamUpdate: {
            update = {
                calcParams: action.data.calcParams,
            }
        }
    }
    return {
        ...state,
        ...update,
    }
}

export interface InputStateContext {
    dataSet: DataSet
    canMeasureBeAggregated: (measure: string) => boolean
    /**
     * {
     *     andFilter1: {
     *         orFilter1: { filter },
     *         orFilter2: { filter },
     *     },
     *     andFilter2: {
     *         orFilter1: { filter },
     *         orFilter2: { filter },
     *     },
     * }
     */
    filters: Filters
    /**
     * {
     *     colToAgg: "how to aggregate"
     * }
     */
    aggData: Record<string, string>
    overrides: Record<number, Override>
    hideZeros: boolean
    totals: boolean
    calcParams: Record<string, string>
    dispatcher: (params: { type: InputStateUpdate; data: Data }) => void
}

const InputContext = createContext<InputStateContext | undefined>(undefined)

export const InputStateContextProvider = InputContext.Provider

export const useInputs = (): InputStateContext => {
    const ctx = useContext(InputContext)
    if (ctx === undefined) {
        throw Error("InputStateContext is undefined")
    }
    return ctx
}


export const buildRequest = (context: InputStateContext, frtb: { measures: {measure: string, agg: string | null}[] }): GenerateTableDataRequest => {
    const data = context.dataSet
    const measures = data.measuresSelected.map(
        (measure: string): [string, string] => {
            const m = frtb.measures.find((it) => it.measure === measure)!
            const agg: string = context.aggData[m.measure]
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
            return [m.measure, agg ?? m.agg]
        },
    )
    return {
        filters: mapFilters(context.filters),
        groupby: data.groupby,
        measures,
        overrides: Object.values(context.overrides),
        hide_zeros: context.hideZeros,
        totals: context.totals,
        calc_params: context.calcParams,
    }
}
