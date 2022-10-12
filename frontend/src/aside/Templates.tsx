import {
    FormControl, IconButton, Box,
    InputLabel,
    MenuItem,
    Select,
    SelectChangeEvent, Tooltip, DialogTitle, DialogContent, TextField, DialogActions, Button, Dialog,
} from "@mui/material"
import { useTemplates } from "../api/hooks"
import { Dispatch, SetStateAction, useId, useRef, useState } from "react"
import { Template } from "../api/types"
import { InputStateUpdate, useInputs } from "./InputStateContext"
import LaunchIcon from "@mui/icons-material/Launch"

const JSONTemplateDialog = (props: {
    open: [boolean, Dispatch<SetStateAction<boolean>>]
}) => {
    const [open, setOpen] = props.open
    const inputs = useInputs()

    const textFieldRef = useRef<HTMLTextAreaElement | null>(null)

    const handleClose = () => {
        setOpen(false)
    }

    const setTemplate = () => {
        if (textFieldRef.current) {
            inputs.dispatcher({
                type: InputStateUpdate.TemplateSelect,
                data: JSON.parse(textFieldRef.current.value),
            })
        }
        handleClose()
    }
    return (
        <Dialog open={open} onClose={handleClose} scroll="paper" fullWidth>
            <DialogTitle>Custom Template</DialogTitle>
            <DialogContent>
                <TextField
                    multiline
                    label="Copied JSON"
                    inputRef={textFieldRef}
                />
            </DialogContent>
            <DialogActions>
                <Button onClick={setTemplate}>OK</Button>
            </DialogActions>
        </Dialog>
    )
}

export const Templates = () => {
    const templates = useTemplates()
    const inputs = useInputs()
    const [selectedTemplate, setSelectedTemplate] = useState<Template | undefined>(undefined)

    const handleChange = (event: SelectChangeEvent) => {
        const name = event.target.value
        const foundTemplate = templates.find((it) => it.name === name)
        if (foundTemplate === undefined) {
            return
        }
        setSelectedTemplate(foundTemplate)
        inputs.dispatcher({
            type: InputStateUpdate.TemplateSelect,
            data: foundTemplate,
        })
    }

    const id = useId()
    const labelId = `${id}-label`

    const [dialogOpen, setDialogOpen] = useState(false)

    const openJsonSelectorDialog = () => {
        setDialogOpen(true)
    }

    return (
        <>
            <Box sx={{ display: "flex" }}>
                <FormControl fullWidth variant="filled">
                    <InputLabel id={labelId}>Templates</InputLabel>
                    <Select
                        labelId={labelId}
                        id={id}
                        value={selectedTemplate?.name ?? ""}
                        label="Templates"
                        onChange={handleChange}
                    >
                        {templates.map((template) => (
                            <MenuItem value={template.name} key={template.name}>
                                {template.name}
                            </MenuItem>
                        ))}
                    </Select>
                </FormControl>
                <Tooltip title="Use custom template">
                    <IconButton onClick={openJsonSelectorDialog}>
                        <LaunchIcon />
                    </IconButton>
                </Tooltip>
            </Box>
            <JSONTemplateDialog open={[dialogOpen, setDialogOpen]} />
        </>
    )
}
