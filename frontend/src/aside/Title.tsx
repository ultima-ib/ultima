import { ButtonBase, Paper, Typography } from "@mui/material"
import { PropsWithChildren, ElementType } from "react"

export default function Title(
    props: PropsWithChildren<{
        content: string
        onClick?: () => void
        component?: ElementType
    }>,
) {
    const clickable = props.onClick !== undefined

    const Component = props.component ?? Paper
    return (
        <Component
            sx={{
                width: "100%",
                textAlign: "center",
                py: 0.5,
                px: 2,
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                ...(clickable ? { cursor: "pointer" } : {}),
            }}
            onClick={props.onClick}
        >
            <Typography
                component={ButtonBase}
                disableRipple
                focusRipple={false}
                sx={{
                    my: 1.5,
                    ...(clickable
                        ? {}
                        : { cursor: "inherit", userSelect: "inherit" }),
                }}
            >
                {props.content}
            </Typography>
            {props.children}
        </Component>
    )
}
