import React, { useState } from 'react'
import {Box, List, ListItem, ListItemText,ListItemButton, Stack, Typography} from "@mui/material";
import {
  DragDropContext,
  Draggable,
  DraggableProvided,
  DraggableStateSnapshot,
  Droppable,
  DroppableProvided, DroppableStateSnapshot, DropResult
} from "@hello-pangea/dnd";
import {Initial} from "./board";


interface InnerListProps {
  dropProvided: DroppableProvided;
  items: number[],
  title: string | undefined | null;
}

function InnerList(props: InnerListProps) {
  const { items, dropProvided } = props;
  const title = props.title;

  return (
      <div>
        {title}
        <div ref={dropProvided.innerRef}>
          {
            items.map((value, index) => (
                <Draggable key={value.toString()} draggableId={value.toString()} index={index}>
                  {(
                      dragProvided: DraggableProvided,
                      dragSnapshot: DraggableStateSnapshot,
                  ) => (
                      <ListItem disablePadding ref={dragProvided.innerRef}>
                        <ListItemButton>
                          <ListItemText primary={`Trash ${value}`} />
                        </ListItemButton>
                      </ListItem>
                  )}
                </Draggable>
            ))
          }
          {dropProvided.placeholder}
        </div>
      </div>
  );
}


const ListSection = () => {

  return (
      <section>
        <Stack spacing={2}>
          <Typography sx={{ mt: 4, mb: 2 }} variant="h6" component="div">
            Text only
          </Typography>
          <List dense>
            <Droppable
                droppableId={'1'}
                type={'listType'}
                ignoreContainerClipping={true}
                isDropDisabled={false}>
              {(
                  dropProvided: DroppableProvided,
                  dropSnapshot: DroppableStateSnapshot,
              ) => (
                  <div
                  >
                    <InnerList
                        title={"fuck"}
                        dropProvided={dropProvided}
                        items={(new Array(12)).fill(undefined).map((_, index) => index)}
                    />
                  </div>
              )}
            </Droppable>
          </List>
        </Stack>
      </section>
  )
}

function App() {
  const onDragEnd= (result: DropResult) => {
    console.log(result)
  }

  return (
    <Initial />
  )
}

export default App
