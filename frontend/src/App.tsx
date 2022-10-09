import AppWrapper from "./AppWrapper"
import useMediaQuery from "@mui/material/useMediaQuery"
import { createTheme, ThemeProvider } from "@mui/material/styles"
import CssBaseline from "@mui/material/CssBaseline"
import { createContext, useContext, useMemo, useState } from "react"

// eslint-disable-next-line @typescript-eslint/no-empty-function
const ColorModeContext = createContext({ toggleColorMode: () => {} })

export const useColorMode = () => useContext(ColorModeContext)

function App() {
    const prefersDarkMode = useMediaQuery("(prefers-color-scheme: dark)")

    const [mode, setMode] = useState<"light" | "dark">(() =>
        prefersDarkMode ? "dark" : "light",
    )

    const colorMode = useMemo(
        () => ({
            toggleColorMode: () => {
                setMode((prevMode) => (prevMode === "light" ? "dark" : "light"))
            },
        }),
        [],
    )

    const theme = useMemo(
        () =>
            createTheme({
                palette: {
                    mode,
                },
            }),
        [mode],
    )
    return (
        <ColorModeContext.Provider value={colorMode}>
            <ThemeProvider theme={theme}>
                <style
                    dangerouslySetInnerHTML={{
                        __html: `html { --color: rgba(${
                            mode === "light" ? "255, 255, 255" : "0, 0, 0"
                        }, 0.5); }`,
                    }}
                ></style>
                <CssBaseline>
                    <AppWrapper />
                </CssBaseline>
            </ThemeProvider>
        </ColorModeContext.Provider>
    )
}

export default App
