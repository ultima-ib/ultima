import Aside from './board';
import {MutableRefObject, useReducer, useRef, useState} from "react";
import {DataSet, Filter as FilterType} from "./types";
import {AggContext, AggType} from "./AggTypes";
import {useFRTB} from "../api/hooks";

function aggTypesReducer(state: { [p: string]: AggType }, action: any) {
    return {
        ...state,
        [action.field]: action.agg
    }
}

export const Initial = () => {
    const frtb = useFRTB();

    const dataSet = useState<DataSet>(() => ({
        fields: frtb.fields,
        measures: frtb.measures.map(it => it.measure),
        groupby: [],
        overwrites: [],
        measuresSelected: [],
        canBeAggregated: (measure: string) => {
            const m = frtb.measures.find(it => it.measure === measure)
            console.log(m, measure)
            return m !== undefined && m.agg === null
        }
    }))

    const filters: MutableRefObject<{ [p: number]: { [p: number]: FilterType } }> = useRef({})

    const [aggData, aggDataUpdater] = useReducer(aggTypesReducer, {})

    const hideZeros = useState(false);
    const totals = useState(false);

    const run = () => {
        const data = dataSet[0]
        const measures: { [p: string]: string }  = {}
        data.measuresSelected.forEach((measure) => {
            const m = frtb.measures.find(it => it.measure === measure)
            if (!m) return
            // @ts-ignore
            const agg: string = aggData[m.measure as any];
            measures[m.measure] = agg ?? m.agg
        })
        const obj = {
            filters: Object.values(filters.current).map(it => Object.values(it)),
            groupby: data.groupby,
            measures,
            overrides: data.overwrites,
            hide_zeros: hideZeros[0],
            totals: totals[0],
        }
        console.log(JSON.stringify(obj, null, 2))
    }

    return (
        <div>
            <AggContext.Provider value={{
                data: aggData,
                updater: aggDataUpdater
            }}>
                <Aside dataSet={dataSet} filters={filters} hideZeros={hideZeros} totals={totals} calcParams={frtb.calcParams} />
            </AggContext.Provider>
            <button onClick={run}>run</button>
        </div>
    )
}

export { Aside }

export default Initial
