import { CalcParam } from "../types"

export enum CalcParamsReducerActionType {
    SetValue,
    AddCalcParam,
}

export interface CalcParamsReducerState {
    list: CalcParam[]
    // name: <value>
    calcParams: Record<string, string>
}

export interface AddAction {
    param: CalcParam
}

export interface SetValueAction {
    name: string
    value: string
}

export interface CalcParamsReducerAction {
    type: CalcParamsReducerActionType
    data: SetValueAction | AddAction
}

export function reducer(
    prevState: CalcParamsReducerState,
    action: CalcParamsReducerAction,
): CalcParamsReducerState {
    switch (action.type) {
        case CalcParamsReducerActionType.SetValue: {
            const data = action.data as SetValueAction
            return {
                ...prevState,
                calcParams: {
                    ...prevState.calcParams,
                    [data.name]: data.value,
                },
            }
        }
        case CalcParamsReducerActionType.AddCalcParam: {
            const data = action.data as AddAction
            const selected = data.param
            return {
                ...prevState,
                calcParams: {
                    ...prevState.calcParams,
                    [selected.name]: "",
                },
            }
        }
    }
    return { list: [], calcParams: {} }
}
