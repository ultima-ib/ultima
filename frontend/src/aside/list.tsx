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
  onClick?: () => void
}

function FieldListItem({ field, provided, onClick }: FieldListItemProps) {
  return (
      <ListItem
          disablePadding
          component="div"
          {...provided.draggableProps}
          {...provided.dragHandleProps}
          ref={provided.innerRef}
      >
        <ListItemButton dense onClick={onClick}>
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
  onListItemClick?: (field: string) => void
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
              <Box sx={{ display: 'flex', alignItems: 'flex-end' }}>
                <FieldListItem
                    key={field}
                    field={field}
                    isDragging={dragSnapshot.isDragging}
                    provided={dragProvided}
                    onClick={() => props.onListItemClick?.(field)}
                />
                {Extras && <Extras field={field}/>}
              </Box>
          )}
        </Draggable>
    );
  }, [fields, props.onListItemClick])

  return (
      <Box sx={{ width: '100%', height: props.height }}>
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
  height: string,
  extras?: any
  onListItemClick?: (field: string) => void
}

export default function FieldList({extras, listId = 'LIST', listType, fields,  height,  onListItemClick}: Props) {

  return (
    <Droppable
      mode="virtual"
      droppableId={listId}
      type={listType}
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
              extras={extras}
              onListItemClick={onListItemClick}
              {...dropProvided.droppableProps}
          />
      )}
    </Droppable>
  );
}
