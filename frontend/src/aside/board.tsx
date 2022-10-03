import React, {Suspense} from 'react';
import type {DraggableLocation, DropResult} from '@hello-pangea/dnd';
import {DragDropContext} from '@hello-pangea/dnd';
import {reorderQuoteMap} from './reorder';
import {Box, Checkbox, FormControlLabel, Stack, Tab, Tabs, TextField} from "@mui/material";
import QuoteList from "./list";
import Title from "./Title";
import {Filters} from "./Filters";
import type {DataSet} from "./types";
import Agg from "./AggTypes";
import {InputStateUpdate, useInputs} from "./InputStateContext";

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
      <div
          role="tabPanel"
          hidden={value !== index}
          id={`tabPanel-${index}`}
          aria-labelledby={`simple-tab-${index}`}
          {...other}
      >
        {value === index && children}
      </div>
  );
}

function a11yProps(index: number) {
  return {
    id: `tab-${index}`,
    'aria-controls': `tabPanel-${index}`,
  };
}


interface ColumnProps {
  title: string;
  fields: string[];
  listId: string,
  height: string,
  extras?: any
  onListItemClick?: (field: string) => void
}

export function Column({ title, fields, listId, height, extras, onListItemClick }: ColumnProps) {
  return (
      <Stack spacing={2} alignItems='center'>
        <Title content={title} />
        <QuoteList
            listId={listId}
            listType="QUOTE"
            fields={fields}
            height={height}
            extras={extras}
            onListItemClick={onListItemClick}
        />
      </Stack>
  );
}

const SearchBox = () => {
  return <></>
}

const FcBoard = (props: {
    onCalcParamsChange: (name: string, value: string) => void
}) => {
  const inputs = useInputs();
  const columns = inputs.dataSet;
  const onDragEnd = (result: DropResult): void => {
    const source: DraggableLocation = result.source;
    if (result.destination === null) {
      if (!(source.droppableId === "fields" || source.droppableId === "measures")) {
        const list: any[] = columns[source.droppableId as keyof DataSet];
        list.splice(source.index, 1)
        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
              // @ts-expect-error mismatched signature
              dataSet: {
                [source.droppableId]: list
              }
            }
        })
      }
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

  const [activeTab, setActiveTab] = React.useState(0);

  const handleActiveTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setActiveTab(newValue);
  };

  const addToList = (list: keyof DataSet, what: string) => {
      inputs.dispatcher({
          type: InputStateUpdate.DataSet,
          data: {
              // @ts-expect-error mismatched signature
              dataSet: {
                  [list]: [what, ...columns[list]]
              }
          }
      })
  }
  return (
      <>
        <DragDropContext onDragEnd={onDragEnd}>
          <Box component='aside' sx={{display: 'flex', gap: 2, width: '35%'}}>
            <Stack  sx={{width: '40%'}}>
              <SearchBox />
              <Column
                  title="Measures"
                  fields={columns.measures} // apply search
                  listId='measures'
                  height={'300px'}
                  onListItemClick={(field) => {
                      console.log('measures: clicked on', field)
                      addToList('measuresSelected', field)
                  }}
              />
              <Column
                  title="Fields"
                  fields={columns.fields} // apply search
                  listId='fields'
                  height={'300px'}
                  onListItemClick={(field) => {
                      console.log('fields: clicked on', field)
                      addToList('groupby', field)
                  }}
              />
            </Stack>
            <Stack sx={{width: '60%'}}>
              <Box sx={{ width: '100%', height: '100%' }}>
                <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
                  <Tabs value={activeTab} onChange={handleActiveTabChange} aria-label="basic tabs example">
                    <Tab label="Item One" {...a11yProps(0)} />
                    <Tab label="Item Two" {...a11yProps(1)} />
                  </Tabs>
                </Box>
                <TabPanel value={activeTab} index={0}>
                  <Column
                      title="Group By"
                      fields={columns.groupby ?? []}
                      listId='groupby'
                      height={'7rem'}
                  />
                  <Column
                      title="Overwrites"
                      fields={columns.overwrites ?? []}
                      listId='overwrites'
                      height={'7rem'}
                  />
                  <Column
                      title="Measures"
                      fields={columns.measuresSelected ?? []}
                      listId='measuresSelected'
                      height={'7rem'}
                      extras={({field}: { field: string }) => (inputs.canMeasureBeAggregated(field) ? (
                          <Suspense>
                            <Agg field={field} />
                          </Suspense>
                      ) : (<></>)) }
                  />
                  <Filters />
                </TabPanel>
                <TabPanel value={activeTab} index={1}>
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
                  <Box sx={{ overflowY: 'scroll', maxHeight: '85vh' }}>
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
              </Box>
            </Stack>
          </Box>
        </DragDropContext>
      </>
  )
}
export default FcBoard
