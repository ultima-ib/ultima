import { createContext, useContext } from "react"
import { DataSet, Filter, Filter as FilterType, Override } from "./types"
import type { Template } from "../api/types"
import { Filters } from "./filters/reducer"

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
            console.log(data)
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
            Object.entries(data.calc_params).forEach(([name, value]) => {
                state.calcParamsUpdater(name, value)
            })

            const aggData: Record<string, string> = {}
            data.measures.forEach(([key, value]) => {
                aggData[key] = value
            })
            update = {
                dataSet,
                filters: buildFilters(data.filters),
                hideZeros: data.hide_zeros,
                totals: data.totals,
                aggData,
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
    calcParamsUpdater: (name: string, value: string) => void
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
