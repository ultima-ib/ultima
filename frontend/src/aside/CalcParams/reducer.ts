import { CalcParam } from "../types"

export enum CalcParamsReducerActionType {
    SetValue,
    AddCalcParam,
}

export interface CalcParamsReducerState {
    available: CalcParam[]
    selected: CalcParam[]
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
            console.log(data)
            const selected = data.param
            return {
                available: prevState.available.filter((it) => it !== selected),
                selected: [...prevState.selected, selected],
                calcParams: {
                    ...prevState.calcParams,
                    [selected.name]: "data.value",
                },
            }
        }
    }
    return { available: [], selected: [], calcParams: {} }
}
