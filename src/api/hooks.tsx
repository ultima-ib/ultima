import {AGG_TYPES, FRTB} from "./routes";
import useFetch from 'fetch-suspense';
import {useEffect, useState} from "react";

interface FRTB {
    fields: string[]
    measures: { [p: string]: string | null }
}

export const useFRTB = () => {
    const resp = useFetch(FRTB) as FRTB
    return {
        fields: resp.fields,
        measures: Object.entries(resp.measures).map(([measure, agg]) => {
            return { measure, agg }
        })
    }
}

export const useAggTypes = () => {
    return useFetch(AGG_TYPES) as string[]
}
export const useFilterColumns = (column: string, search: string = '.*') => {
    const results = useFetch(`${FRTB}/${column}?page=0&pattern=${search === '' ? '.*' : search}`)
    if (results === '') {
        return []
    } else {
        return results as string []
    }
}
