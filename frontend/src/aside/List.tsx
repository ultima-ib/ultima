import React, { PropsWithChildren, ReactElement, useCallback, useEffect, useState } from "react"
import { Virtuoso } from "react-virtuoso"
import { DragDropContext, Droppable, Draggable, DraggableProvided, DropResult } from "@hello-pangea/dnd"
import { Checkbox, ListItem, ListItemButton, ListItemText } from "@mui/material"
import { InputStateUpdate, useInputs } from "./InputStateContext"

// Virtuoso's resize observer can this error,
// which is caught by DnD and aborts dragging.
window.addEventListener("error", (e) => {
    if (e.message === "ResizeObserver loop completed with undelivered notifications." || e.message === "ResizeObserver loop limit exceeded") {
        e.stopImmediatePropagation()
    }
})

type ItemExtras = (v: { field: string }) => ReactElement

function reorder<T>(list: T[], startIndex: number, endIndex: number): T[] {
    const result = Array.from(list)
    const [removed] = result.splice(startIndex, 1)
    result.splice(endIndex, 0, removed)

    return result
}

function Item({
                  provided,
                  item,
                  isDragging,
                  list,
                  extras: Extras,
              }: { provided: DraggableProvided, item: string, isDragging: boolean, list: "measuresSelected" | "groupby", extras?: ItemExtras }) {
    const inputs = useInputs()

    const toggleFromList = (item: string) => {
        const oldList = inputs.dataSet[list]
        const newList = (oldList.includes(item)) ? oldList.filter(it => it !== item) : [...oldList, item]

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error signature mismatch
                dataSet: {
                    [list]: newList,
                },
            },
        })
    }


    return (
        <div style={{ paddingBottom: "8px" }}>
            <ListItem
                {...provided.draggableProps}
                {...provided.dragHandleProps}
                ref={provided.innerRef}
                style={provided.draggableProps.style}
                className={`item ${isDragging ? "is-dragging" : ""}`}
            >
                <ListItemButton sx={{ cursor: "inherit" }} onClick={() => toggleFromList(item)}>
                    <Checkbox
                        edge="start"
                        checked={inputs.dataSet[list].includes(item)}
                        tabIndex={-1}
                        disableRipple
                    />
                    <ListItemText primary={item} />
                </ListItemButton>
                {Extras && <Extras field={item} />}
            </ListItem>
        </div>
    )
}

const HeightPreservingItem = ({ children, ...props }: PropsWithChildren<any>) => {
    const [size, setSize] = useState(0)
    const knownSize = props["data-known-size"]
    useEffect(() => {
        setSize((prevSize) => {
            return knownSize == 0 ? prevSize : knownSize
        })
    }, [knownSize])
    return (
        <div
            {...props}
            className="height-preserving-container"
            // check styling in the style tag below
            style={{ "--child-height": `${size}px` }}
        >
            {children}
        </div>
    )
}

export default function TheList({ readFrom, list, searchValue, extras }: {
    readFrom: "fields" | "measures",
    list: "measuresSelected" | "groupby",
    extras?: ItemExtras,
    searchValue: string
}) {
    const inputs = useInputs()

    const doSearch = useCallback((orElse: string[]) => {
        if (searchValue) {
            const results = orElse.filter((it) =>
                it.toLowerCase().includes(searchValue.toLowerCase()),
            )
            if (results.length >= 0) {
                return results
            } else {
                return []
            }
        } else {
            return orElse
        }
    }, [searchValue])

    const items = doSearch([
        ...inputs.dataSet[list],
        ...inputs.dataSet[readFrom].filter(it => !inputs.dataSet[list].includes(it)),
    ])

    const onDragEnd = useCallback((result: DropResult) => {
        if (!result.destination) {
            return
        }
        if (result.source.index === result.destination.index) {
            return
        }

        if (!inputs.dataSet[list].includes(result.draggableId)) {
            return
        }

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error signature mismatch
                dataSet: {
                    [list]: reorder(inputs.dataSet[list], result.source.index, result.destination!.index),
                },
            },
        })
    }, [inputs.dataSet[list], inputs.dispatcher])

    return (
        <>
            <style>
                {`.height-preserving-container:empty {
                    min-height: calc(var(--child-height));
                    box-sizing: border-box;
                  }`}
            </style>
            <DragDropContext onDragEnd={onDragEnd}>
                <Droppable
                    droppableId="droppable"
                    mode="virtual"
                    renderClone={(provided, snapshot, rubric) => (
                        <Item
                            provided={provided}
                            isDragging={snapshot.isDragging}
                            item={items[rubric.source.index]}
                            list={list}
                            extras={extras}
                        />
                    )}
                >
                    {(provided) => {
                        return (
                            <Virtuoso
                                components={{
                                    Item: HeightPreservingItem,
                                }}
                                // @ts-ignore
                                scrollerRef={provided.innerRef}
                                data={items}
                                style={{ height: 500 }}
                                itemContent={(index, item) => {
                                    return (
                                        <Draggable draggableId={item} index={index} key={item}>
                                            {(provided) => <Item
                                                provided={provided}
                                                item={item}
                                                isDragging={false}
                                                list={list}
                                                extras={extras}
                                            />}
                                        </Draggable>
                                    )
                                }}
                            />
                        )
                    }}
                </Droppable>
            </DragDropContext>
        </>
    )
}
