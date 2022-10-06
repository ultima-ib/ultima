import {
    ChangeEvent,
    PropsWithChildren, ReactElement,
    Suspense,
    SyntheticEvent,
    useDeferredValue,
    useEffect,
    useState
} from 'react';
import type {DraggableLocation, DropResult} from '@hello-pangea/dnd';
import {DragDropContext} from '@hello-pangea/dnd';
import {reorderQuoteMap} from './reorder';
import {
    Accordion as MuiAccordion,
    AccordionDetails,
    AccordionProps,
    AccordionSummary,
    Box,
    BoxProps,
    Checkbox,
    FormControlLabel,
    IconButton,
    Stack,
    StackProps,
    Tab,
    Tabs,
    TextField
} from "@mui/material";
import QuoteList from "./list";
import Title from "./Title";
import {Filters} from "./Filters";
import type {DataSet} from "./types";
import Agg from "./AggTypes";
import {InputStateUpdate, useInputs} from "./InputStateContext";
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import {Resizable as ReResizable} from "re-resizable";
import DeleteIcon from '@mui/icons-material/Close';

const ResizeHandle = () => {
    return <div
        style={{
            background: 'var(--color)',
            height: '100%',
            width: '1px',
        }}
    />
}

const Resizable = (props: PropsWithChildren) => (
    <ReResizable
        handleComponent={{
            right: <ResizeHandle/>
        }}
        minWidth='300px'
        defaultSize={{width: '40%', height: '100%'}}
        enable={{
            top: false,
            right: true,
            bottom: false,
            left: false,
            topRight: false,
            bottomRight: false,
            bottomLeft: false,
            topLeft: false
        }}
        style={{
            display: 'flex', gap: '1em', minWidth: '300px'
        }}
    >
        {props.children}
    </ReResizable>
)

interface TabPanelProps extends BoxProps {
    index: number;
    value: number;
}

function TabPanel(props: TabPanelProps) {
    const {children, value, index, ...other} = props;

    return (
        <Box
            role="tabPanel"
            hidden={value !== index}
            id={`tabPanel-${index}`}
            aria-labelledby={`tab-${index}`}
            {...other}
        >
            {value === index && children}
        </Box>
    );
}

function a11yProps(index: number) {
    return {
        id: `tab-${index}`,
        'aria-controls': `tabPanel-${index}`,
    };
}


interface ColumnProps extends StackProps {
    title?: string;
    fields: string[];
    listId: string,
    extras?: (v: { field: string}) => ReactElement
    onListItemClick?: (field: string) => void
    multiColumn?: boolean,
}

export function Column({title, fields, listId, height, extras, onListItemClick, multiColumn, ...stack}: ColumnProps) {
    return (
        <Stack spacing={2} alignItems='center' {...stack}>
            {title && <Title content={title}/>}
            <QuoteList
                listId={listId}
                listType="QUOTE"
                fields={fields}
                extras={extras}
                onListItemClick={onListItemClick}
                multiColumn={multiColumn ?? false}
            />
        </Stack>
    );
}

const Accordion = ({
                       title,
                       children,
                       hideExpandButton,
                       ...rest
                   }: AccordionProps & { title: string, hideExpandButton?: boolean }) => (
    <MuiAccordion {...rest}>
        <AccordionSummary expandIcon={!hideExpandButton && <ExpandMoreIcon/>} sx={{my: 0}}>
            {title}
        </AccordionSummary>
        <AccordionDetails sx={{
            minHeight: '100px',
            '.MuiAccordionDetails-root': {
                px: 1,
            },
            '.MuiListItemButton-root': {
                px: 1,
            }
        }}>
            {children}
        </AccordionDetails>
    </MuiAccordion>
)

const AccordionColumn = ({title, expanded, onAccordionStateChange: onChange, ...rest}: ColumnProps & { expanded?: boolean, onAccordionStateChange?: (event: SyntheticEvent, expanded: boolean) => void; }) => {
    return (
        <Accordion expanded={expanded} title={title ?? ''} onChange={onChange}>
            <Column
                {...rest}
            />
        </Accordion>
    )
}

const SearchBox = (props: { onChange: (text: string) => void }) => {
    const [searchText, setSearchText] = useState('');
    const onSearchTextChange = (event: ChangeEvent<HTMLInputElement>) => {
        setSearchText(event.target.value);
    }
    const deferredSearchText = useDeferredValue(searchText)
    useEffect(() => {
        props.onChange(deferredSearchText)
    }, [deferredSearchText]);

    return <TextField value={searchText} onChange={onSearchTextChange} label="Search" sx={{my: 1, mx: 1}}
                      variant='filled'></TextField>
}

const DeleteButton = (props: { field: string, from: keyof Omit<DataSet, 'calcParams'>}) => {
    const inputs = useInputs();

    const onDelete = () => {
        const returnTo = props.from === 'measuresSelected' ? 'measures' : 'fields'
        const fromList = inputs.dataSet[props.from].filter(it => it !== props.field);
        const orgList = inputs.dataSet[returnTo];
        const toList = orgList.includes(props.field) ? orgList : [...orgList, props.field]

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error signature mismatch
                dataSet: {
                    [props.from]: fromList,
                    [returnTo]: toList
                }
            }
        })
    }

    return (
        <IconButton onClick={onDelete}>
            <DeleteIcon/>
        </IconButton>
    )
}

const FcBoard = (props: {
    onCalcParamsChange: (name: string, value: string) => void
}) => {
    const inputs = useInputs();
    const columns = inputs.dataSet;
    const onDragEnd = (result: DropResult): void => {
        const source: DraggableLocation = result.source;
        // dragged nowhere, bail
        if (result.destination === null) {
            return;
        }
        const destination: DraggableLocation = result.destination;

        // did not move anywhere - can bail early
        if (source.droppableId === destination.droppableId && source.index === destination.index) {
            return;
        }

        if (destination.droppableId === "fields" || destination.droppableId === "measures") {
            return
        }
        const data = reorderQuoteMap({
            quoteMap: columns,
            source,
            destination,
        });

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                dataSet: {
                    ...inputs.dataSet,
                    ...data
                }
            }
        })
    };

    const [activeTab, setActiveTab] = useState(0);

    const [searchValue, setSearchValue] = useState<string | undefined>(undefined);

    const [measuresAccordionExpanded, setMeasuresAccordionExpanded] = useState(false);
    const [groupByAccordionExpanded, setGroupByAccordionExpanded] = useState(false);

    const doSearch = (orElse: string[]) => {
        if (searchValue) {
            const results = orElse.filter(it => it.toLowerCase().includes(searchValue.toLowerCase()))
            if (results.length >= 0) {
                return results
            } else {
                return []
            }
        } else {
            return orElse
        }
    }
    const handleActiveTabChange = (event: SyntheticEvent, newValue: number) => {
        setActiveTab(newValue);
    };

    const addToList = (list: 'measuresSelected' | 'groupby', what: string, from: keyof Omit<DataSet, 'calcParams'>) => {
        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error mismatched signature
                dataSet: {
                    [from]: columns[from].filter(it => it !== what),
                    [list]: [...columns[list], what]
                }
            }
        })
        if (list === 'measuresSelected') {
            setMeasuresAccordionExpanded(true)
        } else {
            setGroupByAccordionExpanded(true)
        }
    }
    return (
        <DragDropContext onDragEnd={onDragEnd}>
            <Resizable>
                <Stack sx={{width: '40%'}}>
                    <SearchBox onChange={v => setSearchValue(v)}/>
                    <Column
                        title="Measures"
                        fields={doSearch(columns.measures)}
                        listId='measures'
                        sx={{height: '45%'}}
                        onListItemClick={(field) => {
                            addToList('measuresSelected', field, 'measures')
                        }}
                    />
                    <Column
                        title="Fields"
                        fields={doSearch(columns.fields)}
                        listId='fields'
                        sx={{height: '45%'}}
                        onListItemClick={(field) => {
                            addToList('groupby', field, 'fields')
                        }}
                    />
                </Stack>
                <Stack sx={{width: '60%', height: '100%'}}>
                    <Box sx={{borderBottom: 1, borderColor: 'divider'}}>
                        <Tabs value={activeTab} onChange={handleActiveTabChange}>
                            <Tab label="Aggregate" {...a11yProps(0)} />
                            <Tab label="Params" {...a11yProps(1)} />
                        </Tabs>
                    </Box>
                    <TabPanel value={activeTab} index={0} sx={{height: '100%', overflow: 'auto'}}>
                        <AccordionColumn
                            expanded={groupByAccordionExpanded}
                            title="Group By"
                            fields={columns.groupby ?? []}
                            listId='groupby'
                            sx={{height: '20%'}}
                            multiColumn
                            extras={({field}) => <DeleteButton field={field} from='groupby' />}
                            onAccordionStateChange={(event: SyntheticEvent, isExpanded: boolean) => {
                                setGroupByAccordionExpanded(isExpanded)
                            }}
                        />
                        <AccordionColumn
                            title="Overwrites"
                            fields={columns.overwrites ?? []}
                            listId='overwrites'
                            sx={{height: '20%'}}
                            multiColumn
                            extras={({field}) => <DeleteButton field={field} from='overwrites' />}
                        />
                        <AccordionColumn
                            expanded={measuresAccordionExpanded}
                            onAccordionStateChange={(event: SyntheticEvent, isExpanded: boolean) => {
                                setMeasuresAccordionExpanded(isExpanded)
                            }}
                            title="Measures"
                            fields={columns.measuresSelected ?? []}
                            listId='measuresSelected'
                            sx={{height: '20%'}}
                            extras={({field}) => (<>
                                {inputs.canMeasureBeAggregated(field) &&
                                    <Suspense>
                                        <Agg field={field}/>
                                    </Suspense>
                                }
                                <DeleteButton field={field} from='measuresSelected'/>
                            </>)}
                        />
                        <Accordion title="Filters" expanded hideExpandButton>
                            <Filters/>
                        </Accordion>
                    </TabPanel>
                    <TabPanel value={activeTab} index={1} sx={{height: '100%'}}>
                        <Box>
                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.hideZeros}
                                        onChange={(e) => inputs.dispatcher({
                                            type: InputStateUpdate.HideZeros,
                                            data: {hideZeros: e.target.checked}
                                        })}
                                    />
                                }
                                label="Hide Zeros"
                            />

                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.totals}
                                        onChange={(e) => inputs.dispatcher({
                                            type: InputStateUpdate.Total,
                                            data: {totals: e.target.checked}
                                        })}
                                    />
                                }
                                label="Totals"
                            />
                        </Box>
                        <Box sx={{overflowY: 'auto', maxHeight: '80vh'}}>
                            {
                                columns.calcParams.map((it) => (
                                    <TextField
                                        key={it.name}
                                        label={it.name}
                                        defaultValue={it.defaultValue}
                                        helperText={it.helperText}
                                        onChange={(e) => {
                                            props.onCalcParamsChange?.(it.name, e.target.value)
                                        }}
                                        variant="filled"
                                    />
                                ))
                            }
                        </Box>
                    </TabPanel>
                </Stack>
            </Resizable>
        </DragDropContext>
    )
}
export default FcBoard
