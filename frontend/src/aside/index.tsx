import Aside from './board';
import {useReducer} from "react";
import {Filter} from "./types";
import {useFRTB} from "../api/hooks";
import {InputStateContextProvider, inputStateReducer} from "./InputStateContext";


export const Initial = () => {
    const frtb = useFRTB();

    const init = {
        dataSet: {
            fields: frtb.fields,
            measures: frtb.measures.map(it => it.measure),
            groupby: [],
            overwrites: [],
            measuresSelected: [],
            calcParams: frtb.calcParams
        },
        canMeasureBeAggregated: (measure: string) => {
            const m = frtb.measures.find(it => it.measure === measure)
            return m !== undefined && m.agg === null
        },
        filters: {},
        aggData: {},
        hideZeros: false,
        totals: false,
        calcParams: [],
    }
    // @ts-expect-error: i can't get the signature in line with the type declaration
    const [context, dispatcher] = useReducer(inputStateReducer, init);

    const run = () => {
        const data = context.dataSet
        const measures: { [p: string]: string }  = {}
        data.measuresSelected.forEach((measure: string) => {
            const m = frtb.measures.find(it => it.measure === measure)
            if (!m) return
            const agg: string = context.aggData[m.measure as any];
            measures[m.measure] = agg ?? m.agg
        })
        const obj = {
            filters: Object.values(context.filters).map((it: any) => Object.values(it) as Filter[]),
            groupby: data.groupby,
            measures,
            overrides: data.overwrites,
            hide_zeros: context.hideZeros,
            totals: context.totals,
            calc_params: context.calcParams,
        }
        console.log(JSON.stringify(obj, null, 2))
    }

    return (
        <div>
            <InputStateContextProvider value={{
                ...context,
                dispatcher
            }}>
                <Aside />
            </InputStateContextProvider>
            <button onClick={run}>run</button>
        </div>
    )
}

export { Aside }

export default Initial
