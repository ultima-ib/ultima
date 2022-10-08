import {AGG_TYPES, FRTB, COLUMNS} from "./routes";
import useFetch from 'fetch-suspense';
import {GenerateTableDataRequest, GenerateTableDataResponse} from "./types";

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
    const results = useFetch(`${COLUMNS}/${column}?page=0&pattern=${search === '' ? '.*' : search}`)
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
