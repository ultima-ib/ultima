import {MenuItem, FormControl, InputLabel, Select} from '@mui/material';
import type { SelectChangeEvent } from '@mui/material/Select';
import {useId} from "react";
import {useAggTypes} from "../api/hooks";
import {InputStateUpdate, useInputs} from "./InputStateContext";

const Agg = (props: {
    field: string
}) => {
    const ctx = useInputs()
    const handleChange = (event: SelectChangeEvent) => {
        if (event.target.value === '') {
            return
        }
        ctx.dispatcher({
            type: InputStateUpdate.AggData,
            data: {
                aggData: {
                    [props.field]: event.target.value
                }
            }
        })
    };

    const aggTypes = useAggTypes()

    const id = useId();

    return (
        <FormControl variant="filled" sx={{ minWidth: 120 }}>
            <InputLabel id={id}>Agg Type</InputLabel>
            <Select
                labelId={id}
                value={ctx.aggData[props.field] ?? ''}
                onChange={handleChange}
                label="Agg Type"
                required
            >
                {aggTypes.map(it => (
                    <MenuItem value={it} key={it}>{it}</MenuItem>
                ))}
            </Select>
        </FormControl>
    )
}

export default Agg
