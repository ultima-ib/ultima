import { CalcParam } from "../types"
import { Autocomplete, Box, IconButton, TextField } from "@mui/material"
import AddIcon from "@mui/icons-material/Add"
import { ContextProvider, useCalcParamsContext } from "./CalcParamsContext"
import { CalcParamsReducerActionType } from "./reducer"
import { useState } from "react"

const CalcParamsList = () => {
    const { state, dispatch } = useCalcParamsContext()

    return (
        <>
            {state.selected.map((it) => (
                <TextField
                    key={it.name}
                    label={it.name}
                    value={state.calcParams[it.name]}
                    helperText={it.helperText}
                    fullWidth
                    onChange={(e) => {
                        dispatch({
                            type: CalcParamsReducerActionType.SetValue,
                            data: {
                                name: it.name,
                                value: e.target.value,
                            },
                        })
                    }}
                    variant="filled"
                />
            ))}
        </>
    )
}

const AddCalcParam = () => {
    const { state, dispatch } = useCalcParamsContext()
    const [value, setValue] = useState<CalcParam | null>(null)

    const handleAdd = () => {
        if (value === null) {
            return
        }
        dispatch({
            type: CalcParamsReducerActionType.AddCalcParam,
            data: {
                param: value,
            },
        })
        setValue(null)
    }

    return (
        <Box sx={{ display: "flex", pb: 3 }}>
            <Autocomplete
                disablePortal
                sx={{ flex: 1 }}
                options={state.available}
                getOptionLabel={(it) => it.name}
                value={value}
                onChange={(event, newValue) => {
                    setValue(newValue)
                }}
                renderInput={(params) => (
                    <TextField {...params} variant="filled" label={"Field"} />
                )}
            />
            <IconButton onClick={handleAdd}>
                <AddIcon />
            </IconButton>
        </Box>
    )
}

const CalcParams = () => {
    return (
        <ContextProvider>
            <AddCalcParam />
            <CalcParamsList />
        </ContextProvider>
    )
}

export default CalcParams
