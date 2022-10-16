import { MenuItem, FormControl, InputLabel, Select } from "@mui/material"
import { useId, useEffect } from "react"
import { useAggTypes } from "../api/hooks"
import { InputStateUpdate, useInputs } from "./InputStateContext"

const Agg = (props: { field: string }) => {
    const ctx = useInputs()
    const handleChange = (value: string) => {
        if (value === "") {
            return
        }
        ctx.dispatcher({
            type: InputStateUpdate.AggData,
            data: {
                aggData: {
                    [props.field]: value,
                },
            },
        })
    }

    const aggTypes = useAggTypes()

    const id = useId()

    useEffect(() => {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (ctx.aggData?.[props.field] === undefined) {
            const sum = aggTypes.find((it) => it.toLowerCase() === "sum")
            handleChange(sum ?? "")
        }
    }, [])

    return (
        <FormControl variant="filled" sx={{ minWidth: 120 }}>
            <InputLabel id={id}>Agg Type</InputLabel>
            <Select
                labelId={id}
                // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
                value={ctx.aggData[props.field] ?? ""}
                onChange={(e) => handleChange(e.target.value)}
                label="Agg Type"
                required
            >
                {aggTypes.map((it) => (
                    <MenuItem value={it} key={it}>
                        {it}
                    </MenuItem>
                ))}
            </Select>
        </FormControl>
    )
}

export default Agg
