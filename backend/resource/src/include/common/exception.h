/**
 * exception.h
 */

#pragma once

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>



namespace sjtu {
//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum ExceptionType {
  EXCEPTION_TYPE_INVALID = 0,           // invalid type
  EXCEPTION_TYPE_OUT_OF_RANGE = 1,      // value out of range error
  EXCEPTION_TYPE_CONVERSION = 2,        // conversion/casting error
  EXCEPTION_TYPE_UNKNOWN_TYPE = 3,      // unknown type
  EXCEPTION_TYPE_DECIMAL = 4,           // decimal related
  EXCEPTION_TYPE_MISMATCH_TYPE = 5,     // type mismatch
  EXCEPTION_TYPE_DIVIDE_BY_ZERO = 6,    // divide by 0
  EXCEPTION_TYPE_OBJECT_SIZE = 7,       // object size exceeded
  EXCEPTION_TYPE_INCOMPATIBLE_TYPE = 8, // incompatible for operation
  EXCEPTION_TYPE_SERIALIZATION = 9,     // serialization
  EXCEPTION_TYPE_TRANSACTION = 10,      // transaction management
  EXCEPTION_TYPE_NOT_IMPLEMENTED = 11,  // method not implemented
  EXCEPTION_TYPE_EXPRESSION = 12,       // expression parsing
  EXCEPTION_TYPE_CATALOG = 13,          // catalog related
  EXCEPTION_TYPE_PARSER = 14,           // parser related
  EXCEPTION_TYPE_PLANNER = 15,          // planner related
  EXCEPTION_TYPE_SCHEDULER = 16,        // scheduler related
  EXCEPTION_TYPE_EXECUTOR = 17,         // executor related
  EXCEPTION_TYPE_CONSTRAINT = 18,       // constraint related
  EXCEPTION_TYPE_INDEX = 19,            // index related
  EXCEPTION_TYPE_STAT = 20,             // stat related
  EXCEPTION_TYPE_CONNECTION = 21,       // connection related
  EXCEPTION_TYPE_SYNTAX = 22,           // syntax related
};

class Exception : public std::runtime_error {
public:
  Exception(std::string message="error")
      : std::runtime_error(message), type(EXCEPTION_TYPE_INVALID) {
    std::string exception_message = "Message :: " + message + "\n";
    std::cerr << exception_message;
  }

  Exception(ExceptionType exception_type, std::string message)
      : std::runtime_error(message), type(exception_type) {
    std::string exception_message = "\nException Type :: " +
                                    ExpectionTypeToString(exception_type) +
                                    "\nMessage :: " + message + "\n";
    std::cerr << exception_message;
  }

  std::string ExpectionTypeToString(ExceptionType type) {
    switch (type) {
    case EXCEPTION_TYPE_INVALID:
      return "Invalid";
    case EXCEPTION_TYPE_OUT_OF_RANGE:
      return "Out of Range";
    case EXCEPTION_TYPE_CONVERSION:
      return "Conversion";
    case EXCEPTION_TYPE_UNKNOWN_TYPE:
      return "Unknown Type";
    case EXCEPTION_TYPE_DECIMAL:
      return "Decimal";
    case EXCEPTION_TYPE_MISMATCH_TYPE:
      return "Mismatch Type";
    case EXCEPTION_TYPE_DIVIDE_BY_ZERO:
      return "Divide by Zero";
    case EXCEPTION_TYPE_OBJECT_SIZE:
      return "Object Size";
    case EXCEPTION_TYPE_INCOMPATIBLE_TYPE:
      return "Incompatible type";
    case EXCEPTION_TYPE_SERIALIZATION:
      return "Serialization";
    case EXCEPTION_TYPE_TRANSACTION:
      return "Transaction";
    case EXCEPTION_TYPE_NOT_IMPLEMENTED:
      return "Not implemented";
    case EXCEPTION_TYPE_EXPRESSION:
      return "Expression";
    case EXCEPTION_TYPE_CATALOG:
      return "Catalog";
    case EXCEPTION_TYPE_PARSER:
      return "Parser";
    case EXCEPTION_TYPE_PLANNER:
      return "Planner";
    case EXCEPTION_TYPE_SCHEDULER:
      return "Scheduler";
    case EXCEPTION_TYPE_EXECUTOR:
      return "Executor";
    case EXCEPTION_TYPE_CONSTRAINT:
      return "Constraint";
    case EXCEPTION_TYPE_INDEX:
      return "Index";
    case EXCEPTION_TYPE_STAT:
      return "Stat";
    case EXCEPTION_TYPE_CONNECTION:
      return "Connection";
    case EXCEPTION_TYPE_SYNTAX:
      return "Syntax";
    default:
      return "Unknown";
    }
  }

private:
  // type
  ExceptionType type;
};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

//class CastException : public Exception {
//  CastException() = delete;
//
//public:
//  CastException(const TypeId origType, const TypeId newType)
//      : Exception(EXCEPTION_TYPE_CONVERSION,
//                  "Type " + Type::TypeIdToString(origType) +
//                      " can't be cast as " + Type::TypeIdToString(newType)) {}
//};
//
//class ValueOutOfRangeException : public Exception {
//  ValueOutOfRangeException() = delete;
//
//public:
//  ValueOutOfRangeException(const int64_t value, const TypeId origType,
//                           const TypeId newType)
//      : Exception(EXCEPTION_TYPE_CONVERSION,
//                  "Type " + Type::TypeIdToString(origType) + " with value " +
//                      std::to_string((intmax_t)value) +
//                      " can't be cast as %s because the value is out of range "
//                      "for the destination type " +
//                      Type::TypeIdToString(newType)) {}
//
//  ValueOutOfRangeException(const double value, const TypeId origType,
//                           const TypeId newType)
//      : Exception(EXCEPTION_TYPE_CONVERSION,
//                  "Type " + Type::TypeIdToString(origType) + " with value " +
//                      std::to_string(value) +
//                      " can't be cast as %s because the value is out of range "
//                      "for the destination type " +
//                      Type::TypeIdToString(newType)) {}
//
//  ValueOutOfRangeException(const TypeId varType, const size_t length)
//      : Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
//                  "The value is too long to fit into type " +
//                      Type::TypeIdToString(varType) + "(" +
//                      std::to_string(length) + ")"){};
//};

class ConversionException : public Exception {
  ConversionException() = delete;

public:
  ConversionException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONVERSION, msg) {}
};

class UnknownTypeException : public Exception {
  UnknownTypeException() = delete;

public:
  UnknownTypeException(int type, std::string msg)
      : Exception(EXCEPTION_TYPE_UNKNOWN_TYPE,
                  "unknown type " + std::to_string(type) + msg) {}
};

class DecimalException : public Exception {
  DecimalException() = delete;

public:
  DecimalException(std::string msg) : Exception(EXCEPTION_TYPE_DECIMAL, msg) {}
};

//class TypeMismatchException : public Exception {
//  TypeMismatchException() = delete;
//
//public:
//  TypeMismatchException(std::string msg, const TypeId type_1,
//                        const TypeId type_2)
//      : Exception(EXCEPTION_TYPE_MISMATCH_TYPE,
//                  "Type " + Type::TypeIdToString(type_1) +
//                      " does not match with " + Type::TypeIdToString(type_2) +
//                      msg) {}
//};

class NumericValueOutOfRangeException : public Exception {
  NumericValueOutOfRangeException() = delete;

public:
  // internal flags
  static const int TYPE_UNDERFLOW = 1;
  static const int TYPE_OVERFLOW = 2;

  NumericValueOutOfRangeException(std::string msg, int type)
      : Exception(EXCEPTION_TYPE_OUT_OF_RANGE,
                  msg + " " + std::to_string(type)) {}
};

class DivideByZeroException : public Exception {
  DivideByZeroException() = delete;

public:
  DivideByZeroException(std::string msg)
      : Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, msg) {}
};

class ObjectSizeException : public Exception {
  ObjectSizeException() = delete;

public:
  ObjectSizeException(std::string msg)
      : Exception(EXCEPTION_TYPE_OBJECT_SIZE, msg) {}
};

//class IncompatibleTypeException : public Exception {
//  IncompatibleTypeException() = delete;
//
//public:
//  IncompatibleTypeException(int type, std::string msg)
//      : Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE,
//                  "Incompatible type " +
//                      Type::TypeIdToString(static_cast<TypeId>(type)) + msg) {}
//};

class SerializationException : public Exception {
  SerializationException() = delete;

public:
  SerializationException(std::string msg)
      : Exception(EXCEPTION_TYPE_SERIALIZATION, msg) {}
};

class TransactionException : public Exception {
  TransactionException() = delete;

public:
  TransactionException(std::string msg)
      : Exception(EXCEPTION_TYPE_TRANSACTION, msg) {}
};

class NotImplementedException : public Exception {
  NotImplementedException() = delete;

public:
  NotImplementedException(std::string msg)
      : Exception(EXCEPTION_TYPE_NOT_IMPLEMENTED, msg) {}
};

class ExpressionException : public Exception {
  ExpressionException() = delete;

public:
  ExpressionException(std::string msg)
      : Exception(EXCEPTION_TYPE_EXPRESSION, msg) {}
};

class CatalogException : public Exception {
  CatalogException() = delete;

public:
  CatalogException(std::string msg) : Exception(EXCEPTION_TYPE_CATALOG, msg) {}
};

class ParserException : public Exception {
  ParserException() = delete;

public:
  ParserException(std::string msg) : Exception(EXCEPTION_TYPE_PARSER, msg) {}
};

class PlannerException : public Exception {
  PlannerException() = delete;

public:
  PlannerException(std::string msg) : Exception(EXCEPTION_TYPE_PLANNER, msg) {}
};

class SchedulerException : public Exception {
  SchedulerException() = delete;

public:
  SchedulerException(std::string msg)
      : Exception(EXCEPTION_TYPE_SCHEDULER, msg) {}
};

class ExecutorException : public Exception {
  ExecutorException() = delete;

public:
  ExecutorException(std::string msg)
      : Exception(EXCEPTION_TYPE_EXECUTOR, msg) {}
};

class SyntaxException : public Exception {
  SyntaxException() = delete;

public:
  SyntaxException(std::string msg) : Exception(EXCEPTION_TYPE_SYNTAX, msg) {}
};

class ConstraintException : public Exception {
  ConstraintException() = delete;

public:
  ConstraintException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONSTRAINT, msg) {}
};

class IndexException : public Exception {
  IndexException() = delete;

public:
  IndexException(std::string msg) : Exception(EXCEPTION_TYPE_INDEX, msg) {}
};

class StatException : public Exception {
  StatException() = delete;

public:
  StatException(std::string msg) : Exception(EXCEPTION_TYPE_STAT, msg) {}
};

class ConnectionException : public Exception {
  ConnectionException() = delete;

public:
  ConnectionException(std::string msg)
      : Exception(EXCEPTION_TYPE_CONNECTION, msg) {}
};

} // namespace sjtu
