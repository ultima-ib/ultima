import {
    Accordion as MuiAccordion,
    AccordionDetails,
    AccordionProps,
    AccordionSummary,
} from "@mui/material"
import ExpandMoreIcon from "@mui/icons-material/ExpandMore"

const Accordion = ({
    title,
    children,
    hideExpandButton,
    ...rest
}: AccordionProps & { title: string; hideExpandButton?: boolean }) => (
    <MuiAccordion {...rest}>
        <AccordionSummary
            expandIcon={!hideExpandButton && <ExpandMoreIcon />}
            sx={{ my: 0 }}
        >
            {title}
        </AccordionSummary>
        <AccordionDetails
            sx={{
                minHeight: "100px",
                ".MuiAccordionDetails-root": {
                    px: 1,
                },
                ".MuiListItemButton-root": {
                    px: 1,
                },
            }}
        >
            {children}
        </AccordionDetails>
    </MuiAccordion>
)

export default Accordion
