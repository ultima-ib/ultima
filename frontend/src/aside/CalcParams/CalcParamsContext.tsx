import {
    createContext,
    PropsWithChildren,
    useContext,
    useEffect,
    useReducer,
} from "react"
import { fancyFilter } from "../../utils"
import {
    CalcParamsReducerAction,
    CalcParamsReducerState,
    reducer,
} from "./reducer"
import { InputStateUpdate, useInputs } from "../InputStateContext"

export interface CalcParamsContext {
    dispatch: (action: CalcParamsReducerAction) => void
    state: CalcParamsReducerState
}

const Context = createContext<CalcParamsContext | null>(null)

export const ContextProvider = (props: PropsWithChildren<never>) => {
    const inputs = useInputs()

    const [state, dispatch] = useReducer(
        reducer,
        { available: [], selected: [], calcParams: {} },
        (): CalcParamsReducerState => {
            const [withDefaults, withoutDefaults] = fancyFilter(
                inputs.dataSet.calcParams,
                (it) => it.defaultValue !== null,
            )
            const calcParams: Record<string, string> = {}
            withDefaults.forEach((it) => {
                calcParams[it.name] = it.defaultValue!
            })
            return {
                available: withoutDefaults,
                selected: withDefaults,
                calcParams,
            }
        },
    )

    useEffect(() => {
        inputs.dispatcher({
            type: InputStateUpdate.CalcParamUpdate,
            data: {
                calcParams: state.calcParams,
            },
        })
    }, [state.calcParams])

    return (
        <Context.Provider value={{ state, dispatch }}>
            {props.children}
        </Context.Provider>
    )
}

export const useCalcParamsContext = (): CalcParamsContext =>
    useContext(Context)!
