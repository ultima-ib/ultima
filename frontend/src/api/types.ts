import {Filter, Override} from '../aside/types';

interface BaseTableData {
    calc_params: { [p: string]: string };
    filters: Filter[][];
    groupby: string[];
    overrides: Override[];
    hide_zeros: boolean;
    totals: boolean;
}

export interface GenerateTableDataRequest extends BaseTableData {
    measures: { [p: string]: string };
}

export interface GenerateTableDataResponse {
    columns: {
        name: string,
        datatype: string,
        values: (string | number | null)[]
    }[],
}

export interface Template extends BaseTableData {
    measures: [string, string][]
    name: string
}
