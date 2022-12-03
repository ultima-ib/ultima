import { Filters } from "./filters"
import { Filters as FiltersType } from "./filters/reducer"
import Title from "./Title"
import {
    Autocomplete,
    Box,
    Button,
    Dialog,
    DialogActions,
    DialogContent,
    DialogTitle,
    TextField,
} from "@mui/material"
import LaunchIcon from "@mui/icons-material/Launch"
import { Dispatch, SetStateAction, useState } from "react"
import { InputStateUpdate, useInputs } from "./InputStateContext"
import { Override } from "./types"
import { mapFilters } from "../utils"
import { useOverrides } from "../api/hooks"

let overrideUsed = 0

const OverridesDialog = (props: {
    open: [boolean, Dispatch<SetStateAction<boolean>>]
}) => {
    const [open, setOpen] = props.open
    const inputs = useInputs()

    const handleClose = () => {
        setOpen(false)
    }

    const handleSetOverrides = () => {
        setOpen(false)
    }

    const fields = useOverrides()

    const updateOverride = (
        index: number,
        field: string | undefined,
        value: string | undefined,
        filters: FiltersType,
    ) => {
        inputs.dispatcher({
            type: InputStateUpdate.Overrides,
            data: {
                overrides: {
                    [index]: {
                        field,
                        value,
                        filters: mapFilters(filters),
                    },
                },
            },
        })
    }
    const addOverride = () => {
        overrideUsed += 1
        inputs.dispatcher({
            type: InputStateUpdate.Overrides,
            data: {
                overrides: {
                    [overrideUsed]: {
                        field: undefined,
                        value: undefined,
                        filters: [],
                    },
                },
            },
        })
    }
    return (
        <div>
            <Dialog open={open} onClose={handleClose} scroll="paper" fullWidth>
                <DialogTitle>Overrides</DialogTitle>
                <DialogContent
                    sx={{ display: "flex", flexDirection: "column", gap: 4 }}
                >
                    {Object.entries(inputs.overrides)
                        .map(([index, override]): [number, Override] => [
                            index as unknown as number,
                            override,
                        ])
                        .map(([index, override]) => (
                            <Box key={index}>
                                <Autocomplete
                                    disablePortal
                                    options={fields}
                                    value={override.field ?? null}
                                    onChange={(event, newValue) => {
                                        updateOverride(
                                            index,
                                            newValue as unknown as string,
                                            override.value,
                                            override.filters,
                                        )
                                    }}
                                    renderInput={(params) => (
                                        <TextField
                                            {...params}
                                            variant="filled"
                                            label={"Field"}
                                        />
                                    )}
                                />
                                <TextField
                                    autoFocus
                                    margin="dense"
                                    label="New value"
                                    fullWidth
                                    variant="filled"
                                    value={override.value ?? ""}
                                    onChange={(event) => {
                                        updateOverride(
                                            index,
                                            override.field,
                                            event.target.value,
                                            override.filters,
                                        )
                                    }}
                                />
                                <Filters
                                    component={Box}
                                    onFiltersChange={(filters) => {
                                        updateOverride(
                                            index,
                                            override.field,
                                            override.value,
                                            filters,
                                        )
                                    }}
                                    fields={fields}
                                />
                            </Box>
                        ))}
                </DialogContent>
                <DialogActions>
                    <Button onClick={addOverride}>Add Override</Button>
                    <Button onClick={handleSetOverrides}>OK</Button>
                </DialogActions>
            </Dialog>
        </div>
    )
}

export function Overrides() {
    const [dialogOpen, setDialogOpen] = useState(false)

    return (
        <>
            <Title content="Overrides" onClick={() => setDialogOpen(true)}>
                <>
                    <LaunchIcon />
                </>
            </Title>
            <OverridesDialog open={[dialogOpen, setDialogOpen]} />
        </>
    )
}
