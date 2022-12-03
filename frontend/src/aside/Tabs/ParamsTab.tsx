import { Box, Checkbox, FormControlLabel } from "@mui/material"
import { InputStateUpdate, useInputs } from "../InputStateContext"
import CalcParams from "../CalcParams"

export const ParamsTab = () => {
    const inputs = useInputs()

    return (
        <>
            <Box>
                <FormControlLabel
                    control={
                        <Checkbox
                            checked={inputs.hideZeros}
                            onChange={(e) =>
                                inputs.dispatcher({
                                    type: InputStateUpdate.HideZeros,
                                    data: {
                                        hideZeros: e.target.checked,
                                    },
                                })
                            }
                        />
                    }
                    label="Hide Zeros"
                />

                <FormControlLabel
                    control={
                        <Checkbox
                            checked={inputs.totals}
                            onChange={(e) =>
                                inputs.dispatcher({
                                    type: InputStateUpdate.Total,
                                    data: {
                                        totals: e.target.checked,
                                    },
                                })
                            }
                        />
                    }
                    label="Totals"
                />
            </Box>
            <Box sx={{ overflowY: "auto", maxHeight: "80vh" }}>
                <CalcParams />
            </Box>
        </>
    )
}

export default ParamsTab
