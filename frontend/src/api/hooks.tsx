import {AGG_TYPES, FRTB} from "./routes";
import useFetch from 'fetch-suspense';
import {useEffect, useState} from "react";
import {GenerateTableDataRequest, GenerateTableDataResponse} from "./types";
import response from '../responce.json'

interface FRTB {
    fields: string[]
    measures: { [p: string]: string | null }
    calc_params: {
        name: string,
        default: string,
        type_hint: string
    }[]
}

export const useFRTB = () => {
    const resp = useFetch(FRTB) as FRTB
    return {
        fields: resp.fields,
        measures: Object.entries(resp.measures).map(([measure, agg]) => {
            return { measure, agg,  }
        }),
        calcParams: resp.calc_params.map(it => ({
            name: it.name,
            defaultValue: it.default,
            helperText: it.type_hint,
        }))
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

export const useTableData = (input: GenerateTableDataRequest): { data?: GenerateTableDataResponse, error?: string } => {
    const resp = useFetch(FRTB, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(input)
    }, { metadata: true })
    if (resp.status === 200) {
        return { data: resp.response as GenerateTableDataResponse }
    } else {
        return { error: resp.response.toString() }
    }
}
