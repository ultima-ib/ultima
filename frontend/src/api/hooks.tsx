import {
    AGG_TYPES,
    FRTB as FRTB_ROUTE,
    COLUMNS,
    TEMPLATES,
    OVERRIDES,
    DESCRIBE,
} from "./routes"
import useFetch from "fetch-suspense"
import {
    GenerateTableDataRequest,
    GenerateTableDataResponse,
    Template,
} from "./types"

interface FRTB {
    fields: string[]
    measures: Record<string, string | null>
    calc_params: {
        name: string
        default: string
        type_hint: string
    }[]
}

export const useFRTB = () => {
    const resp = useFetch(FRTB_ROUTE) as FRTB
    return {
        fields: resp.fields,
        measures: Object.entries(resp.measures).map(([measure, agg]) => {
            return { measure, agg }
        }),
        calcParams: resp.calc_params.map((it) => ({
            name: it.name,
            defaultValue: it.default,
            helperText: it.type_hint,
        })),
    }
}

export const useAggTypes = () => {
    return useFetch(AGG_TYPES) as string[]
}

export const useOverrides = () => {
    return useFetch(OVERRIDES) as string[]
}

export const useTemplates = () => {
    return useFetch(TEMPLATES) as Template[]
}

export const useFilterColumns = (column: string, search = ".*") => {
    const results = useFetch(
        `${COLUMNS}/${column}?page=0&pattern=${search === "" ? ".*" : search}`,
    )
    if (results === "") {
        return []
    } else {
        return results as string[]
    }
}

export const useTableData = (
    input: GenerateTableDataRequest,
): { data?: GenerateTableDataResponse; error?: string } => {
    const resp = useFetch(
        FRTB_ROUTE,
        {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(input),
        },
        { metadata: true },
    )
    if (resp.status === 200) {
        return { data: resp.response as GenerateTableDataResponse }
    } else {
        if (typeof resp.response === "string") {
            return { error: resp.response }
        } else {
            return {
                error: "Unexpected response; contact system administrator",
            }
        }
    }
}

export const useDescribeTableData = (
    input: GenerateTableDataResponse,
): GenerateTableDataResponse => {
    // const { data } = useTableData(input)
    return useFetch(DESCRIBE, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(input),
    }) as GenerateTableDataResponse
}
