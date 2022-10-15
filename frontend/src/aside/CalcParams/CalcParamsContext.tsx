import {
    createContext,
    PropsWithChildren,
    useContext,
    useEffect,
    useReducer,
} from "react"
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

export const ContextProvider = (props: PropsWithChildren) => {
    const inputs = useInputs()

    const [state, dispatch] = useReducer(
        reducer,
        { available: [], selected: [], calcParams: {} },
        (): CalcParamsReducerState => {
            const calcParams: Record<string, string> = inputs.calcParams
            inputs.dataSet.calcParams
                .filter(
                    (it) =>
                        it.defaultValue !== null &&
                        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
                        calcParams[it.name] === undefined,
                )
                .forEach((it) => {
                    calcParams[it.name] = it.defaultValue!
                })

            return {
                list: inputs.dataSet.calcParams,
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
