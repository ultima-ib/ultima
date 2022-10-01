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

    const run = () => {
        const data = dataSet[0]
        const obj = {
            filters: Object.values(filters.current).map(it => Object.values(it)),
            overwrites: data.overwrites,
            groupby: data.groupby,
            measures: data.measuresSelected
        }
        console.log(JSON.stringify(obj))
    }

    return (
        <div>
            <AggContext.Provider value={{
                data: aggData,
                updater: aggDataUpdater
            }}>
                <Aside dataSet={dataSet} filters={filters} />
            </AggContext.Provider>
            <button onClick={run}>run</button>
        </div>
    )
}

export { Aside }

export default Initial
