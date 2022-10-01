import {useCallback} from 'react';
import { Droppable, Draggable } from '@hello-pangea/dnd';
import type {
  DroppableProvided,
  DraggableProvided,
  DraggableStateSnapshot,
} from '@hello-pangea/dnd';
import {Box, ListItem, ListItemButton, ListItemText} from "@mui/material";
import { Virtuoso } from 'react-virtuoso'

interface FieldListItemProps {
  field: string;
  isDragging: boolean;
  provided: DraggableProvided;
}

function FieldListItem({ field, provided }: FieldListItemProps) {
  return (
      <ListItem
          disablePadding
          component="div"
          {...provided.draggableProps}
          {...provided.dragHandleProps}
          ref={provided.innerRef}
      >
        <ListItemButton dense>
          <ListItemText>
            {field}
          </ListItemText>
        </ListItemButton>
      </ListItem>
  )
}

interface InnerListProps {
  dropProvided: DroppableProvided;
  fields: string[];
  height: string,
  extras?: any
}

function InnerList(props: InnerListProps) {
  const Extras = props.extras;
  const { fields, dropProvided } = props;
  const renderRow = useCallback((index: number) => {
    const field: string = fields[index]
    return (
        <Draggable key={field} draggableId={field} index={index}>
          {(
              dragProvided: DraggableProvided,
              dragSnapshot: DraggableStateSnapshot,
          ) => (
              <Box sx={{ display: 'flex' }}>
                <FieldListItem
                    key={field}
                    field={field}
                    isDragging={dragSnapshot.isDragging}
                    provided={dragProvided}
                />
                {Extras && <Extras field={field}/>}
              </Box>
          )}
        </Draggable>
    );
  }, [fields])

  return (
      <Box sx={{ width: '100%'}}>
        <Virtuoso
            style={{ height: props.height }}
            // @ts-expect-error signature mismatch between libraries
            scrollerRef={dropProvided.innerRef}
            totalCount={fields.length}
            itemContent={index => renderRow(index)}
          />
      </Box>
  );
}


interface Props {
  listId?: string;
  listType?: string;
  fields: string[];
  internalScroll?: boolean;
  isDropDisabled?: boolean;
  // may not be provided - and might be null
  ignoreContainerClipping?: boolean;
  useClone?: boolean;
  height: string,
  extras?: any
}

export default function FieldList(props: Props) {
  const {
    ignoreContainerClipping,
    isDropDisabled,
    listId = 'LIST',
    listType,
    fields,
    height
  } = props;

  return (
    <Droppable
      mode="virtual"
      droppableId={listId}
      type={listType}
      ignoreContainerClipping={ignoreContainerClipping}
      isDropDisabled={isDropDisabled}
      renderClone={(provided, snapshot, descriptor) => {
        return <FieldListItem
            field={fields[descriptor.source.index]}
            provided={provided}
            isDragging={snapshot.isDragging}
        />
      }}
    >
      {(
        dropProvided: DroppableProvided,
      ) => (
          <InnerList
              fields={fields}
              height={height}
              dropProvided={dropProvided}
              extras={props.extras}
              {...dropProvided.droppableProps}
          />
      )}
    </Droppable>
  );
}
