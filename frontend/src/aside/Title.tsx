import {Paper, Typography} from "@mui/material";

export default function Title(props: { content: string }) {
    return (
        <Paper sx={{
            width: '100%',
            textAlign: 'center',
            py: 0.5
        }}>
            <Typography variant='subtitle2'>{props.content}</Typography>
        </Paper>
    )
}
