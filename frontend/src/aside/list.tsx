import { ReactElement, useCallback } from "react"
import { Droppable, Draggable } from "@hello-pangea/dnd"
import type {
    DroppableProvided,
    DraggableProvided,
    DraggableStateSnapshot,
} from "@hello-pangea/dnd"
import {
    Box,
    BoxProps,
    ListItem,
    ListItemButton,
    ListItemText,
} from "@mui/material"
import { styled } from "@mui/material/styles"
import { Virtuoso, VirtuosoGrid } from "react-virtuoso"

const ListItemStyled = styled(ListItem)({
    width: "fit-content",
    maxWidth: "50%",
    "&.MuiListItem-root": {
        paddingLeft: 0,
        paddingRight: 0,
        paddingTop: 0,
    },
})

const ListContainer = styled(Box)<BoxProps>({
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "space-between",
    gap: 1,
})

interface FieldListItemProps {
    field: string
    isDragging: boolean
    provided: DraggableProvided
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
                <ListItemText>{field}</ListItemText>
            </ListItemButton>
        </ListItem>
    )
}

export type ListItemExtras = (v: { field: string }) => ReactElement

interface InnerListProps {
    dropProvided: DroppableProvided
    fields: string[]
    extras?: ListItemExtras
    onListItemClick?: (field: string) => void
    multiColumn: boolean
}

function InnerList(props: InnerListProps) {
    const Extras = props.extras
    const { fields, dropProvided } = props
    const renderRow = useCallback(
        (index: number) => {
            const field: string = fields[index]
            return (
                <Draggable key={field} draggableId={field} index={index}>
                    {(
                        dragProvided: DraggableProvided,
                        dragSnapshot: DraggableStateSnapshot,
                    ) => (
                        <Box sx={{ display: "flex", alignItems: "flex-end" }}>
                            <FieldListItem
                                key={field}
                                field={field}
                                isDragging={dragSnapshot.isDragging}
                                provided={dragProvided}
                                onClick={() => props.onListItemClick?.(field)}
                            />
                            {Extras && <Extras field={field} />}
                        </Box>
                    )}
                </Draggable>
            )
        },
        [fields, props.onListItemClick],
    )

    return (
        <Box sx={{ width: "100%", height: "100%", minHeight: "100px" }}>
            {props.multiColumn ? (
                <VirtuosoGrid
                    style={{ height: "100%", minHeight: "100px" }}
                    scrollerRef={dropProvided.innerRef}
                    totalCount={fields.length}
                    itemContent={(index) => renderRow(index)}
                    components={{
                        Item: ListItemStyled,
                        // @ts-expect-error signature mismatch
                        List: ListContainer,
                        ScrollSeekPlaceholder: () => (
                            <ListItem component="div">--</ListItem>
                        ),
                    }}
                    scrollSeekConfiguration={{
                        enter: (velocity) => Math.abs(velocity) > 200,
                        exit: (velocity) => Math.abs(velocity) < 30,
                        change: (_, range) => console.log({ range }),
                    }}
                />
            ) : (
                <Virtuoso
                    style={{ height: "100%", minHeight: "100px" }}
                    // this should never be window so safely cast it to make tsc happy
                    scrollerRef={(a) =>
                        dropProvided.innerRef(
                            a as unknown as HTMLElement | null,
                        )
                    }
                    totalCount={fields.length}
                    itemContent={(index) => renderRow(index)}
                />
            )}
        </Box>
    )
}

interface Props {
    listId?: string
    listType?: string
    fields: string[]
    extras?: ListItemExtras
    onListItemClick?: (field: string) => void
    multiColumn?: boolean
}

export default function FieldList(props: Props) {
    const {
        extras,
        listId = "LIST",
        multiColumn = false,
        listType,
        fields,
        onListItemClick,
    } = props

    return (
        <Droppable
            mode="virtual"
            droppableId={listId}
            type={listType}
            renderClone={(provided, snapshot, descriptor) => {
                return (
                    <FieldListItem
                        field={fields[descriptor.source.index]}
                        provided={provided}
                        isDragging={snapshot.isDragging}
                    />
                )
            }}
        >
            {(dropProvided: DroppableProvided) => (
                <InnerList
                    fields={fields}
                    dropProvided={dropProvided}
                    extras={extras}
                    onListItemClick={onListItemClick}
                    multiColumn={multiColumn}
                    {...dropProvided.droppableProps}
                />
            )}
        </Droppable>
    )
}
