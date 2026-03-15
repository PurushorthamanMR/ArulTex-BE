const { Op } = require('sequelize');
const { Sale, SaleItem, Product, User, ProductCategory, Supplier, Customer } = require('../models');
const { sequelize } = require('../config/database');
const { recordInventoryChange } = require('./inventoryHelper');
const logger = require('../config/logger');

function saleToDto(row, items = []) {
  if (!row) return null;
  const dto = {
    id: row.id,
    invoiceNo: row.invoiceNo,
    userId: row.userId,
    customerId: row.customerId ?? null,
    subtotal: row.subtotal != null ? Number(row.subtotal) : null,
    totalAmount: row.totalAmount != null ? Number(row.totalAmount) : null,
    paymentMethod: row.paymentMethod,
    saleDate: row.saleDate,
    status: row.status
  };
  if (row.user) dto.user = { id: row.user.id, firstName: row.user.firstName, lastName: row.user.lastName };
  if (row.customer) dto.customer = { id: row.customer.id, customerName: row.customer.customerName, phone: row.customer.phone, email: row.customer.email };
  if (items.length) dto.items = items.map(saleItemToDto);
  else if (row.items) dto.items = row.items.map(saleItemToDto);
  return dto;
}

function saleItemToDto(row) {
  if (!row) return null;
  return {
    id: row.id,
    saleId: row.saleId,
    productId: row.productId,
    quantity: row.quantity,
    unitPrice: row.unitPrice != null ? Number(row.unitPrice) : null,
    discountAmount: row.discountAmount != null ? Number(row.discountAmount) : 0,
    totalPrice: row.totalPrice != null ? Number(row.totalPrice) : null,
    returnedQty: row.returnedQty || 0,
    product: row.product ? { id: row.product.id, productName: row.product.productName } : null
  };
}

async function save(body) {
  logger.info('SaleService.save() invoked');
  const { userId, paymentMethod, items } = body;
  if (!items || !items.length) throw new Error('Sale must have at least one item');
  const invoiceNo = body.invoiceNo || `INV-${Date.now()}`;
  let subtotal = 0;
  const itemRows = items.map(it => {
    const qty = parseInt(it.quantity);
    const unitPrice = Number(it.unitPrice);
    const discountAmount = Number(it.discountAmount) || 0;
    const totalPrice = qty * unitPrice - discountAmount;
    subtotal += totalPrice;
    return { productId: it.productId, quantity: qty, unitPrice, discountAmount, totalPrice };
  });
  const totalAmount = subtotal;
  const sale = await Sale.create({
    invoiceNo,
    userId: userId || body.userId,
    customerId: body.customerId != null ? body.customerId : null,
    subtotal,
    totalAmount,
    paymentMethod,
    saleDate: body.saleDate || new Date(),
    status: 'Completed'
  });
  for (const it of itemRows) {
    await SaleItem.create({
      saleId: sale.id,
      productId: it.productId,
      quantity: it.quantity,
      unitPrice: it.unitPrice,
      discountAmount: it.discountAmount,
      totalPrice: it.totalPrice
    });
    await recordInventoryChange({
      productId: it.productId,
      transactionType: 'Sale',
      quantity: -it.quantity,
      referenceId: sale.id,
      userId: sale.userId,
      note: `Sale ${sale.invoiceNo}`
    });
  }
  const withItems = await Sale.findByPk(sale.id, {
    include: [
      { model: User, as: 'user' },
      { model: Customer, as: 'customer', attributes: ['id', 'customerName', 'phone', 'email'], required: false },
      { model: SaleItem, as: 'items', include: [{ model: Product, as: 'product', attributes: ['id', 'productName'] }] }
    ]
  });
  return saleToDto(withItems);
}

async function update(body) {
  logger.info('SaleService.update() invoked');
  const sale = await Sale.findByPk(body.id, { include: [{ model: SaleItem, as: 'items' }] });
  if (!sale) throw new Error('Sale not found');
  await sale.update({
    customerId: body.customerId !== undefined ? (body.customerId || null) : sale.customerId,
    subtotal: body.subtotal != null ? Number(body.subtotal) : sale.subtotal,
    totalAmount: body.totalAmount != null ? Number(body.totalAmount) : sale.totalAmount,
    paymentMethod: body.paymentMethod ?? sale.paymentMethod,
    status: body.status ?? sale.status
  });
  const customerInclude = { model: Customer, as: 'customer', attributes: ['id', 'customerName', 'phone', 'email'], required: false };
  const withItems = await Sale.findByPk(sale.id, {
    include: [
      { model: User, as: 'user' },
      customerInclude,
      { model: SaleItem, as: 'items', include: [{ model: Product, as: 'product' }] }
    ]
  });
  return saleToDto(withItems);
}

async function getAll() {
  const list = await Sale.findAll({
    include: [
      { model: User, as: 'user' },
      { model: Customer, as: 'customer', attributes: ['id', 'customerName', 'phone', 'email'], required: false },
      { model: SaleItem, as: 'items', include: [{ model: Product, as: 'product' }] }
    ],
    order: [['saleDate', 'DESC']]
  });
  return list.map(saleToDto);
}

async function getById(id) {
  const sale = await Sale.findByPk(id, {
    include: [
      { model: User, as: 'user' },
      { model: Customer, as: 'customer', attributes: ['id', 'customerName', 'phone', 'email'], required: false },
      { model: SaleItem, as: 'items', include: [{ model: Product, as: 'product' }] }
    ]
  });
  return saleToDto(sale);
}

async function search(query) {
  const where = {};
  if (query.invoiceNo) where.invoiceNo = { [Op.like]: `%${query.invoiceNo}%` };
  if (query.userId) where.userId = query.userId;
  if (query.customerId) where.customerId = query.customerId;
  if (query.status) where.status = query.status;
  if (query.fromDate) where.saleDate = { ...where.saleDate, [Op.gte]: query.fromDate };
  if (query.toDate) where.saleDate = { ...where.saleDate, [Op.lte]: query.toDate };
  const list = await Sale.findAll({
    where,
    include: [
      { model: User, as: 'user' },
      { model: Customer, as: 'customer', attributes: ['id', 'customerName', 'phone', 'email'], required: false },
      { model: SaleItem, as: 'items', include: [{ model: Product, as: 'product' }] }
    ],
    order: [['saleDate', 'DESC']]
  });
  return list.map(saleToDto);
}

async function deleteById(id) {
  const sale = await Sale.findByPk(id, { include: [{ model: SaleItem, as: 'items' }] });
  if (!sale) throw new Error('Sale not found');
  await SaleItem.destroy({ where: { saleId: id } });
  await sale.destroy();
  return { id };
}

/** POST /return: body { saleId, items: [{ saleItemId, returnQty }] } - add stock back, update returnedQty */
async function processReturn(body) {
  logger.info('SaleService.processReturn() invoked');
  const { saleId, items } = body;
  if (!items || !items.length) throw new Error('Return must specify items');
  const sale = await Sale.findByPk(saleId, { include: [{ model: SaleItem, as: 'items', include: [{ model: Product, as: 'product' }] }] });
  if (!sale) throw new Error('Sale not found');
  for (const it of items) {
    const saleItem = sale.items.find(i => i.id === it.saleItemId);
    if (!saleItem) continue;
    const returnQty = Math.min(parseInt(it.returnQty) || 0, saleItem.quantity - (saleItem.returnedQty || 0));
    if (returnQty <= 0) continue;
    await saleItem.update({ returnedQty: (saleItem.returnedQty || 0) + returnQty });
    await recordInventoryChange({
      productId: saleItem.productId,
      transactionType: 'Return',
      quantity: returnQty,
      referenceId: saleId,
      userId: body.userId,
      note: `Return from sale ${sale.invoiceNo}`
    });
  }
  // If every item is fully returned, mark sale as Refunded
  const updatedSale = await Sale.findByPk(saleId, { include: [{ model: SaleItem, as: 'items' }] });
  const allReturned = updatedSale.items.every(
    (i) => (Number(i.returnedQty) || 0) >= (Number(i.quantity) || 0)
  );
  if (allReturned && updatedSale.items.length > 0) {
    await updatedSale.update({ status: 'Refunded' });
  }
  return getById(saleId);
}

async function reportDaily(date) {
  const d = date ? new Date(date) : new Date();
  const start = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const end = new Date(start);
  end.setDate(end.getDate() + 1);
  const list = await Sale.findAll({
    where: { saleDate: { [Op.gte]: start, [Op.lt]: end }, status: ['Completed', 'Refunded'] },
    attributes: ['id', 'invoiceNo', 'totalAmount', 'saleDate', 'status']
  });
  const totalSales = list.filter(s => s.status === 'Completed').reduce((sum, s) => sum + Number(s.totalAmount), 0);
  return { date: start.toISOString().slice(0, 10), count: list.length, totalSales, sales: list.map(s => ({ id: s.id, invoiceNo: s.invoiceNo, totalAmount: s.totalAmount, saleDate: s.saleDate, status: s.status })) };
}

async function reportMonthly(year, month) {
  const y = parseInt(year) || new Date().getFullYear();
  const m = parseInt(month) !== undefined ? parseInt(month) : new Date().getMonth() + 1;
  const start = new Date(y, m - 1, 1);
  const end = new Date(y, m, 1);
  const list = await Sale.findAll({
    where: { saleDate: { [Op.gte]: start, [Op.lt]: end }, status: ['Completed', 'Refunded'] },
    attributes: ['id', 'invoiceNo', 'totalAmount', 'saleDate', 'status']
  });
  const totalSales = list.filter(s => s.status === 'Completed').reduce((sum, s) => sum + Number(s.totalAmount), 0);
  return { year: y, month: m, count: list.length, totalSales, sales: list.map(s => ({ id: s.id, invoiceNo: s.invoiceNo, totalAmount: s.totalAmount, saleDate: s.saleDate })) };
}

async function reportByCategory(fromDate, toDate) {
  const where = {};
  if (fromDate) where.saleDate = { ...where.saleDate, [Op.gte]: new Date(fromDate) };
  if (toDate) where.saleDate = { ...where.saleDate, [Op.lte]: new Date(toDate) };
  where.status = 'Completed';
  const rows = await Sale.findAll({
    where,
    include: [{ model: SaleItem, as: 'items', include: [{ model: Product, as: 'product', include: [{ model: ProductCategory, as: 'category' }] }] }]
  });
  const byCat = {};
  for (const sale of rows) {
    for (const item of sale.items || []) {
      const catName = item.product?.category?.categoryName || 'Uncategorized';
      if (!byCat[catName]) byCat[catName] = { categoryName: catName, totalAmount: 0, quantity: 0 };
      byCat[catName].totalAmount += Number(item.totalPrice);
      byCat[catName].quantity += item.quantity;
    }
  }
  return Object.values(byCat);
}

async function reportBySupplier(fromDate, toDate) {
  const where = {};
  if (fromDate) where.saleDate = { ...where.saleDate, [Op.gte]: new Date(fromDate) };
  if (toDate) where.saleDate = { ...where.saleDate, [Op.lte]: new Date(toDate) };
  where.status = 'Completed';
  const rows = await Sale.findAll({
    where,
    include: [{ model: SaleItem, as: 'items', include: [{ model: Product, as: 'product', include: [{ model: Supplier, as: 'supplier' }] }] }]
  });
  const bySup = {};
  for (const sale of rows) {
    for (const item of sale.items || []) {
      const supName = item.product?.supplier?.supplierName || 'No supplier';
      if (!bySup[supName]) bySup[supName] = { supplierName: supName, totalAmount: 0, quantity: 0 };
      bySup[supName].totalAmount += Number(item.totalPrice);
      bySup[supName].quantity += item.quantity;
    }
  }
  return Object.values(bySup);
}

/** GET /report/trends: { daily: [{ date, totalAmount }], monthly: [{ year, month, totalAmount }], yearly: [{ year, totalAmount }] } */
async function reportTrends() {
  const where = { status: 'Completed' };
  const sales = await Sale.findAll({
    where,
    attributes: ['saleDate', 'totalAmount'],
    raw: true
  });

  const dailyMap = {};
  const monthlyMap = {};
  const yearlyMap = {};

  const now = new Date();
  const thirtyDaysAgo = new Date(now);
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

  for (const s of sales) {
    const amt = Number(s.totalAmount) || 0;
    const d = new Date(s.saleDate);
    const dateKey = d.toISOString().slice(0, 10);
    const year = d.getFullYear();
    const month = d.getMonth() + 1;

    if (d >= thirtyDaysAgo) {
      dailyMap[dateKey] = (dailyMap[dateKey] || 0) + amt;
    }
    const monthKey = `${year}-${month}`;
    monthlyMap[monthKey] = (monthlyMap[monthKey] || 0) + amt;
    yearlyMap[year] = (yearlyMap[year] || 0) + amt;
  }

  const daily = Object.entries(dailyMap)
    .map(([date, totalAmount]) => ({ date, totalAmount: Math.round(totalAmount * 100) / 100 }))
    .sort((a, b) => a.date.localeCompare(b.date));
  const monthly = Object.entries(monthlyMap)
    .map(([key, totalAmount]) => {
      const [y, m] = key.split('-').map(Number);
      return { year: y, month: m, totalAmount: Math.round(totalAmount * 100) / 100 };
    })
    .sort((a, b) => a.year !== b.year ? a.year - b.year : a.month - b.month);
  const yearly = Object.entries(yearlyMap)
    .map(([year, totalAmount]) => ({ year: Number(year), totalAmount: Math.round(totalAmount * 100) / 100 }))
    .sort((a, b) => a.year - b.year);

  return { daily, monthly, yearly };
}

/** GET /report/top-products?limit=10: [{ product: { productName }, totalQty, totalRevenue }] */
async function reportTopProducts(limit = 10) {
  const rows = await sequelize.query(
    'SELECT si.productId, SUM(si.quantity) AS totalQty, SUM(si.totalPrice) AS totalRevenue FROM sale_items si INNER JOIN sales s ON s.id = si.saleId AND s.status = ? GROUP BY si.productId ORDER BY totalRevenue DESC LIMIT ?',
    { replacements: ['Completed', Math.min(Number(limit) || 10, 100)], type: sequelize.QueryTypes.SELECT }
  );
  const list = Array.isArray(rows) ? rows : [];
  const productIds = [...new Set(list.map((r) => r.productId))];
  const products = productIds.length
    ? await Product.findAll({ where: { id: productIds }, attributes: ['id', 'productName'] })
    : [];
  const byId = {};
  products.forEach((p) => { byId[p.id] = p; });
  return list.map((r) => ({
    productId: r.productId,
    product: byId[r.productId] ? { id: byId[r.productId].id, productName: byId[r.productId].productName } : null,
    totalQty: Number(r.totalQty) || 0,
    totalRevenue: Math.round((Number(r.totalRevenue) || 0) * 100) / 100
  }));
}

/** GET /report/profitability: [{ categoryName, revenue, profit }] - revenue from sales, profit = revenue - cost from sale items */
async function reportProfitability() {
  const where = { status: 'Completed' };
  const rows = await Sale.findAll({
    where,
    include: [
      {
        model: SaleItem,
        as: 'items',
        include: [
          { model: Product, as: 'product', include: [{ model: ProductCategory, as: 'category' }] }
        ]
      }
    ]
  });
  const byCat = {};
  for (const sale of rows) {
    for (const item of sale.items || []) {
      const catName = item.product?.category?.categoryName || 'Uncategorized';
      if (!byCat[catName]) byCat[catName] = { categoryName: catName, revenue: 0, cost: 0 };
      const revenue = Number(item.totalPrice) || 0;
      const cost = (Number(item.product?.costPrice) || 0) * (item.quantity || 0);
      byCat[catName].revenue += revenue;
      byCat[catName].cost += cost;
    }
  }
  return Object.values(byCat).map((c) => ({
    categoryName: c.categoryName,
    revenue: Math.round(c.revenue * 100) / 100,
    profit: Math.round((c.revenue - c.cost) * 100) / 100
  }));
}

/** GET /report/low-stock: products where stockQty <= minStockLevel */
async function reportLowStock() {
  const products = await Product.findAll({
    where: { isActive: true },
    include: [{ model: ProductCategory, as: 'category', attributes: ['id', 'categoryName'] }],
    attributes: ['id', 'productName', 'stockQty', 'minStockLevel']
  });
  const filtered = products.filter((p) => (Number(p.stockQty) || 0) <= (Number(p.minStockLevel) || 0));
  return filtered.map((p) => ({
    id: p.id,
    productName: p.productName,
    stockQty: p.stockQty,
    minStockLevel: p.minStockLevel,
    category: p.category ? { id: p.category.id, categoryName: p.category.categoryName } : null
  }));
}

module.exports = {
  save,
  update,
  getAll,
  getById,
  search,
  deleteById,
  processReturn,
  reportDaily,
  reportMonthly,
  reportByCategory,
  reportBySupplier,
  reportTrends,
  reportTopProducts,
  reportProfitability,
  reportLowStock
};
