import {Filter, Override} from '../aside/types';

export interface GenerateTableDataRequest {
    measures: { [p: string]: string };
    calc_params: { [p: string]: string };
    filters: Filter[][];
    groupby: string[];
    overrides: Override[];
    hide_zeros: boolean;
    totals: boolean;
}

export interface GenerateTableDataResponse {
    columns: {
        name: string,
        datatype: string,
        values: (string | number | null)[]
    }[],
}
