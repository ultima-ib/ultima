import {createContext, useContext} from "react";
import {CalcParam, DataSet, Filter as FilterType} from "./types";

export enum InputStateUpdate {
    DataSet,
    Filters,
    HideZeros,
    Total,
    AggData,
}
//
// type Data2 = {
//     aggData: any;
//     calcParams: { defaultValue: string; name: string; helperText: string }[];
//     canMeasureBeAggregated: (measure: string) => boolean;
//     filters: {};
//     totals: boolean;
//     hideZeros: boolean;
//     dataSet: { overwrites: any[]; measuresSelected: any[]; measures: string[]; groupby: any[]; fields: string[] }
// }

type Data = Partial<Omit<InputStateContext, 'dispatcher'>>
export function inputStateReducer(state: InputStateContext, action: { type: InputStateUpdate, data: Data }) {
    let update;
    switch (action.type) {
        case InputStateUpdate.DataSet:
            update = {
                dataSet: {
                    ...state.dataSet,
                    ...action.data.dataSet,
                }
            }
            break;
        case InputStateUpdate.Filters:
            update = {
                filters: {
                    ...action.data.filters,
                }
            }
            break;
        case InputStateUpdate.HideZeros:
            update = {
                hideZeros: action.data.hideZeros
            }
            break;
        case InputStateUpdate.Total:
            update = {
                totals: action.data.totals
            }
            break;
        case InputStateUpdate.AggData:
            update = {
                aggData: {
                    ...state.aggData,
                    ...action.data.aggData,
                }
            }
            break;
    }
    return {
        ...state,
        ...update
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
    filters: { [p: number]: { [p: number]: FilterType } }
    /**
     * {
     *     colToAgg: "how to aggregate"
     * }
     */
    aggData: { [p: string]: string }
    hideZeros: boolean
    totals: boolean
    calcParams: CalcParam[]
    dispatcher: (params: { type: InputStateUpdate, data: Data }) => void
}

const InputStateContext = createContext<InputStateContext | undefined>(undefined)

export const InputStateContextProvider = InputStateContext.Provider

export const useInputs = (): InputStateContext => {
    const ctx = useContext(InputStateContext)
    if (ctx === undefined) {
        throw Error("InputStateContext is undefined")
    }
    return ctx
}
