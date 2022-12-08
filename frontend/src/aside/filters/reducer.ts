import { Filter } from "../types"

export type Filters = Record<number, Record<number, Filter>>

export enum ActionType {
    NewAnd,
    NewOr,
    RemoveAnd,
    RemoveOr,
    Update,
    Set,
}

export interface Action {
    type: ActionType
}

export interface AndFilter extends Action {
    index: number
}

export interface OrFilter extends Action {
    andIndex: number
    index: number
}

export interface UpdateFilter extends OrFilter, Filter {}

export interface SetFilters extends Action {
    filters: Filters
}

const EMPTY_FILTER: Filters = {}

export function reducer(
    prevState: Filters,
    action: AndFilter | OrFilter | UpdateFilter | SetFilters,
): Filters {
    switch (action.type) {
        case ActionType.NewAnd: {
            const data = action as AndFilter
            return {
                ...prevState,
                [data.index]: EMPTY_FILTER,
            }
        }
        case ActionType.RemoveAnd: {
            const data = action as AndFilter
            if (Object.keys(prevState[data.index]).length === 0) {
                const copy = { ...prevState }
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete copy[data.index]
                return copy
            }
            return prevState
        }
        case ActionType.NewOr: {
            const data = action as OrFilter
            return {
                ...prevState,
                [data.andIndex]: {
                    ...prevState[data.andIndex],
                    [data.index]: EMPTY_FILTER,
                },
            }
        }
        case ActionType.RemoveOr: {
            const data = action as OrFilter
            const copy = { ...prevState[data.andIndex] }
            // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
            delete copy[data.index]
            return {
                ...prevState,
                [data.andIndex]: copy,
            }
        }
        case ActionType.Update: {
            const { andIndex, index, op, value, field } = action as UpdateFilter
            return {
                ...prevState,
                [andIndex]: {
                    ...prevState[andIndex],
                    [index]: { op, value, field },
                },
            }
        }
        case ActionType.Set: {
            const data = action as SetFilters
            return data.filters
        }
    }
    throw Error("unreachable")
}

export type FiltersReducerDispatch = (
    a: AndFilter | OrFilter | UpdateFilter | SetFilters,
) => void
