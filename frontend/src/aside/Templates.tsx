import {
    FormControl,
    InputLabel,
    MenuItem,
    Select,
    SelectChangeEvent,
} from "@mui/material"
import { useTemplates } from "../api/hooks"
import { useId, useState } from "react"
import { Template } from "../api/types"
import { InputStateUpdate, useInputs } from "./InputStateContext"

export const Templates = () => {
    const templates = useTemplates()
    const inputs = useInputs()
    const [selectedTemplate, setSelectedTemplate] = useState<
        Template | undefined
    >(undefined)

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

    return (
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
    )
}
