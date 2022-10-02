import {MenuItem, FormControl, InputLabel, Select} from '@mui/material';
import type { SelectChangeEvent } from '@mui/material/Select';
import {createContext, useContext, useId} from "react";
import {useAggTypes} from "../api/hooks";

const AGG_TYPES =  [
    "max",
    "count_unique",
    "count",
    "sum",
    "min",
    "quantile95low",
    "var",
    "first",
    "mean"
] as const

export type AggType = typeof AGG_TYPES[number];

export const AggContext = createContext<{
    data: { [p: string]: AggType },
    updater: (t: any) => void
} | undefined>(undefined)

const Agg = (props: {
    field: string
}) => {
    const aggCtx = useContext(AggContext)
    const handleChange = (event: SelectChangeEvent) => {
        if (aggCtx === undefined) {
            console.error('aggCtx is undefined')
            return
        }
        aggCtx.updater({
            field: props.field,
            agg: event.target.value as AggType
        })
    };

    const aggTypes = useAggTypes()

    const id = useId();

    return (
        <FormControl variant="standard" sx={{ minWidth: 120 }}>
            <InputLabel id={id}>Agg Types</InputLabel>
            <Select
                labelId={id}
                value={aggCtx?.data[props.field] ?? ''}
                onChange={handleChange}
                label="Agg Types"
            >
                {aggTypes.map(it => (
                    <MenuItem value={it} key={it}>{it}</MenuItem>
                ))}
            </Select>
        </FormControl>
    )
}

export default Agg
