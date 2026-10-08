import {useMemo, useState} from 'react';
import {flexRender, getCoreRowModel, getPaginationRowModel, getSortedRowModel, useReactTable, type SortingState, type ColumnDef} from '@tanstack/react-table';
import type {Cell, Column} from './types';
import {compareDecimal, formatCell, unitLabel} from './presentation.mjs';

export default function DataTable({columns, rows, caption, metadata = [], rowScope = 'stored rows'}: {columns: string[]; rows: Cell[][]; caption: string; metadata?: Column[]; rowScope?: string}) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const defs = useMemo<ColumnDef<Cell[]>[]>(() => columns.map((name, index) => {
    const column = metadata.find(c => c.name === name);
    return {id: name, accessorFn: row => row[index], header: name.replaceAll('_', ' ') + (column?.unit ? ` (${unitLabel(column.unit)})` : ''),
      cell: info => formatCell(info.getValue(), column?.unit),
      sortingFn: (a, b) => column?.type === 'decimal' || column?.type === 'integer'
        ? compareDecimal(a.original[index], b.original[index]) : String(a.original[index] ?? '').localeCompare(String(b.original[index] ?? ''))};
  }), [columns, metadata]);
  const table = useReactTable({data: rows, columns: defs, state: {sorting}, onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel(), getPaginationRowModel: getPaginationRowModel(),
    initialState: {pagination: {pageSize: 10}}});
  return <div className="table-section">
    <div className="table-scroll" role="region" aria-label={caption} tabIndex={0}><table>
      <caption>{caption}</caption>
      <thead>{table.getHeaderGroups().map(group => <tr key={group.id}>{group.headers.map(header => <th key={header.id} scope="col"
        aria-sort={header.column.getIsSorted() === 'asc' ? 'ascending' : header.column.getIsSorted() === 'desc' ? 'descending' : 'none'}>
        <button className="sort" onClick={header.column.getToggleSortingHandler()}>{flexRender(header.column.columnDef.header, header.getContext())}
          {header.column.getIsSorted() === 'asc' ? ' ↑' : header.column.getIsSorted() === 'desc' ? ' ↓' : ' ↕'}</button></th>)}</tr>)}</thead>
      <tbody>{table.getRowModel().rows.map(row => <tr key={row.id}>{row.getVisibleCells().map(cell => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody>
    </table></div>
    {!rows.length && <p>No rows matched this request.</p>}
    <div className="pagination"><button className="secondary" disabled={!table.getCanPreviousPage()} onClick={() => table.previousPage()}>Previous rows</button>
      <span>Page {table.getState().pagination.pageIndex + 1} of {Math.max(table.getPageCount(), 1)} · {rows.length} {rowScope}</span>
      <button className="secondary" disabled={!table.getCanNextPage()} onClick={() => table.nextPage()}>Next rows</button></div>
    <p className="note">Sorting and pages apply to these {rowScope}.</p>
  </div>;
}
